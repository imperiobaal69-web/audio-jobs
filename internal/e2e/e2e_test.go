package e2e

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imperiobaal69-web/audio-jobs/internal/store"
	"github.com/imperiobaal69-web/audio-jobs/internal/worker"
)

func TestSubmitProcessAndCache(t *testing.T) {
	h := newHarness(t)
	h.startWorker("w1", worker.Config{Concurrency: 2})
	body := wav(1, 4000)

	res := h.submit("acme", "", body)
	if res.Status != http.StatusAccepted || res.Job.Status != store.Queued {
		t.Fatalf("submit: %+v", res)
	}
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Succeeded || job.Attempts != 1 || job.Cached {
		t.Fatalf("job: %+v", job)
	}
	if r := decodeResult(t, job); r.DurationMs != 4000 || len(r.Silences) == 0 {
		t.Fatalf("result: %+v", r)
	}

	// The same recording from another tenant is answered from the cache at
	// submission time, without touching the queue.
	again := h.submit("globex", "", body)
	if again.Status != http.StatusCreated || again.Job.Status != store.Succeeded || !again.Job.Cached {
		t.Fatalf("cached submit: %+v", again)
	}
	if !reflect.DeepEqual(decodeResult(t, again.Job), decodeResult(t, job)) {
		t.Fatal("cached result differs from computed result")
	}
}

func TestIdempotencyKeyReturnsSameJob(t *testing.T) {
	h := newHarness(t)
	first := h.submit("acme", "upload-42", wav(2, 1000))
	second := h.submit("acme", "upload-42", wav(2, 1000))
	other := h.submit("globex", "upload-42", wav(2, 1000)) // keys are per tenant
	if first.Status != http.StatusAccepted || second.Status != http.StatusOK || second.Job.ID != first.Job.ID {
		t.Fatalf("first %+v second %+v", first, second)
	}
	if other.Job.ID == first.Job.ID {
		t.Fatal("idempotency key leaked across tenants")
	}
}

func TestRejectsBadUploads(t *testing.T) {
	h := newHarness(t)
	if r := h.submit("acme", "", []byte("definitely not audio")); r.Status != http.StatusUnsupportedMediaType {
		t.Fatalf("garbage: %+v", r)
	}
	if r := h.submit("acme", "", wav(3, 300_000)); r.Status != http.StatusRequestEntityTooLarge { // 9.6 MB > 8 MB
		t.Fatalf("too large: %+v", r)
	}
	if r := h.submit("", "", wav(3, 100)); r.Status != http.StatusBadRequest {
		t.Fatalf("no tenant: %+v", r)
	}
}

func TestTransientFailuresAreRetriedWithBackoff(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.startWorker("w1", worker.Config{BeforeProcess: func(_ context.Context, _ string, attempt int) error {
		calls.Add(1)
		if attempt < 3 {
			return errors.New("simulated outage")
		}
		return nil
	}})
	res := h.submit("acme", "", wav(4, 1000))
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Succeeded || job.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("job %+v calls=%d", job, calls.Load())
	}
	// Backoff 50 ms then 100 ms (+ up to 20% jitter) must have elapsed.
	if took := job.FinishedAt.Sub(job.CreatedAt); took < 150*time.Millisecond {
		t.Fatalf("finished after %s: backoff not applied", took)
	}
}

func TestExhaustedRetriesGoToDeadLetter(t *testing.T) {
	h := newHarness(t)
	h.startWorker("w1", worker.Config{BeforeProcess: func(context.Context, string, int) error {
		return errors.New("always down")
	}})
	res := h.submit("acme", "", wav(5, 1000))
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Dead || job.Attempts != 3 || deref(job.LastError) != "always down" {
		t.Fatalf("job %+v", job)
	}
	assertDeadLetters(t, h, 1)
}

func TestCorruptInputFailsPermanentlyWithoutRetry(t *testing.T) {
	h := newHarness(t)
	h.startWorker("w1", worker.Config{})
	body := wav(6, 1000)
	// Valid header, but the data chunk claims more bytes than the file has.
	binary.LittleEndian.PutUint32(body[40:44], uint32(len(body)*2))
	res := h.submit("acme", "", body)
	if res.Status != http.StatusAccepted {
		t.Fatalf("submit %+v", res)
	}
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Dead || job.Attempts != 1 {
		t.Fatalf("job %+v", job)
	}
	assertDeadLetters(t, h, 1)
}

func TestCrashedWorkerDeliveryIsReclaimed(t *testing.T) {
	h := newHarness(t)
	res := h.submit("acme", "", wav(7, 1000))

	// A worker takes the delivery and its lease, then dies without acking.
	crashed := h.queue("crashed-pod")
	if err := crashed.Init(h.ctx); err != nil {
		t.Fatal(err)
	}
	msgs, err := crashed.Read(h.ctx, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %v %v", msgs, err)
	}
	if _, ok, err := h.store.Start(h.ctx, msgs[0].JobID, time.Second); !ok || err != nil {
		t.Fatalf("start: %v %v", ok, err)
	}

	h.startWorker("survivor", worker.Config{Visibility: time.Second})
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Succeeded || job.Attempts != 2 {
		t.Fatalf("job %+v", job)
	}
	st, _ := h.queue("x").Stats(h.ctx)
	if st.Pending != 0 {
		t.Fatalf("pending deliveries left: %+v", st)
	}
}

func TestStaleLeaseCannotOverwriteNewerAttempt(t *testing.T) {
	h := newHarness(t)
	res := h.submit("acme", "", wav(8, 500))
	slow, _, _ := h.store.Start(h.ctx, res.Job.ID, time.Minute) // attempt 1, then stalls
	fast, _, _ := h.store.Start(h.ctx, res.Job.ID, time.Minute) // attempt 2 after reclaim
	if err := h.store.Succeed(h.ctx, res.Job.ID, fast.LeaseToken, json.RawMessage(`{"winner":2}`), false); err != nil {
		t.Fatal(err)
	}
	if err := h.store.Succeed(h.ctx, res.Job.ID, slow.LeaseToken, json.RawMessage(`{"winner":1}`), false); !errors.Is(err, store.ErrLostLease) {
		t.Fatalf("stale attempt: want ErrLostLease, got %v", err)
	}
	if _, err := h.store.Fail(h.ctx, res.Job.ID, slow.LeaseToken, "late", false); !errors.Is(err, store.ErrLostLease) {
		t.Fatalf("stale fail: want ErrLostLease, got %v", err)
	}
	if j := h.get("acme", res.Job.ID); string(j.Result) != `{"winner": 2}` || j.Status != store.Succeeded {
		t.Fatalf("job %+v result %s", j, j.Result)
	}
}

func TestSweeperRecoversJobsTheQueueLost(t *testing.T) {
	h := newHarness(t)
	res := h.submit("acme", "", wav(9, 1000))
	// Redis loses everything (restart without persistence).
	keys, _ := h.rdb.Keys(h.ctx, h.prefix+":*").Result()
	h.rdb.Del(h.ctx, keys...)

	h.startWorker("w1", worker.Config{SweepEvery: 100 * time.Millisecond, SweepIdle: 300 * time.Millisecond})
	job := h.waitFor("acme", res.Job.ID, 10*time.Second)
	if job.Status != store.Succeeded {
		t.Fatalf("job %+v", job)
	}
}

func TestShutdownDrainsInFlightJobs(t *testing.T) {
	h := newHarness(t)
	started := make(chan struct{}, 1)
	stop := h.startWorker("w1", worker.Config{BeforeProcess: func(context.Context, string, int) error {
		started <- struct{}{}
		time.Sleep(500 * time.Millisecond) // a long job
		return nil
	}})
	res := h.submit("acme", "", wav(10, 1000))
	<-started
	stop() // SIGTERM equivalent: must wait for the job, not abandon it
	if j := h.get("acme", res.Job.ID); j.Status != store.Succeeded || j.Attempts != 1 {
		t.Fatalf("after shutdown: %+v", j)
	}
}

func TestListUsesKeysetPagination(t *testing.T) {
	h := newHarness(t)
	want := map[string]bool{}
	for i := range 7 {
		want[h.submit("acme", "", wav(uint64(100+i), 200)).Job.ID] = true
	}
	h.submit("globex", "", wav(200, 200))

	seen := map[string]bool{}
	var cursor *store.Cursor
	var pages []int
	for {
		jobs, err := h.store.List(h.ctx, "acme", "", cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, len(jobs))
		for i, j := range jobs {
			if seen[j.ID] || !want[j.ID] {
				t.Fatalf("unexpected or repeated job %s", j.ID)
			}
			seen[j.ID] = true
			if i > 0 && jobs[i-1].CreatedAt.Before(j.CreatedAt) {
				t.Fatal("not newest-first")
			}
		}
		if len(jobs) < 3 {
			break
		}
		last := jobs[len(jobs)-1]
		cursor = &store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(seen) != 7 || len(pages) != 3 || pages[2] != 1 {
		t.Fatalf("pages %v, saw %d jobs", pages, len(seen))
	}
}

func assertDeadLetters(t *testing.T, h *harness, want int64) {
	t.Helper()
	st, err := h.queue("x").Stats(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Dead != want || st.Pending != 0 || st.Delayed != 0 {
		t.Fatalf("queue stats %+v, want %d dead and nothing pending", st, want)
	}
}
