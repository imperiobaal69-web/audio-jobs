// Package worker processes jobs from the queue.
//
// One worker process runs a fetch loop feeding a bounded pool of goroutines,
// plus three maintenance loops: promoting due retries, reclaiming deliveries
// abandoned by crashed workers, and sweeping Postgres for jobs the queue may
// have lost. On shutdown it stops fetching, lets in-flight jobs finish up to
// a deadline, and leaves anything unfinished to be reclaimed elsewhere.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/imperiobaal69-web/audio-jobs/internal/audio"
	"github.com/imperiobaal69-web/audio-jobs/internal/blob"
	"github.com/imperiobaal69-web/audio-jobs/internal/cache"
	"github.com/imperiobaal69-web/audio-jobs/internal/metrics"
	"github.com/imperiobaal69-web/audio-jobs/internal/queue"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
)

// Config tunes a worker.
type Config struct {
	Concurrency       int
	Visibility        time.Duration // lease length and reclaim threshold
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	SweepEvery        time.Duration
	SweepIdle         time.Duration // a queued job untouched this long is re-enqueued
	PromoteEvery      time.Duration
	ChaosFailRate     float64       // probability of an injected transient failure (testing only)
	ChaosProcessDelay time.Duration // extra processing time (testing only)

	// Analyze is the analysis function; nil means audio.Analyze. Load tests
	// set it to audio.AnalyzeReference to measure the baseline end to end.
	Analyze func(io.ReadSeeker, audio.Params) (audio.Result, error)

	// BeforeProcess, if set, runs at the start of every attempt; a returned
	// error fails the attempt as a transient error. Test seam.
	BeforeProcess func(ctx context.Context, jobID string, attempt int) error
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.Visibility <= 0 {
		c.Visibility = 30 * time.Second
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 5 * time.Minute
	}
	if c.SweepEvery <= 0 {
		c.SweepEvery = 30 * time.Second
	}
	if c.SweepIdle <= 0 {
		c.SweepIdle = 2 * time.Minute
	}
	if c.Analyze == nil {
		c.Analyze = audio.Analyze
	}
	if c.PromoteEvery <= 0 {
		c.PromoteEvery = 250 * time.Millisecond
	}
	return c
}

// PermanentError marks failures that retrying cannot fix (corrupt input).
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

type Worker struct {
	cfg   Config
	store *store.Store
	queue *queue.Queue
	blobs *blob.Store
	cache *cache.Results
	log   *slog.Logger
}

func New(cfg Config, s *store.Store, q *queue.Queue, b *blob.Store, c *cache.Results, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg.withDefaults(), store: s, queue: q, blobs: b, cache: c, log: log}
}

// Run processes jobs until ctx is cancelled, then drains in-flight jobs for
// at most drain before returning.
func (w *Worker) Run(ctx context.Context, drain time.Duration) error {
	if err := w.queue.Init(ctx); err != nil {
		return err
	}
	// Processing uses its own context: cancelling ctx stops fetching, not
	// the jobs already running. It is cancelled only if draining times out.
	procCtx, cancelProc := context.WithCancel(context.Background())
	defer cancelProc()

	slots := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup
	dispatch := func(msgs []queue.Message) {
		for _, m := range msgs {
			slots <- struct{}{}
			inflight.Add(1)
			go func(m queue.Message) {
				defer func() { <-slots; inflight.Done() }()
				metrics.InFlight.Inc()
				defer metrics.InFlight.Dec()
				w.handle(procCtx, m)
			}(m)
		}
	}

	var loops sync.WaitGroup
	every := func(d time.Duration, f func()) {
		loops.Add(1)
		go func() {
			defer loops.Done()
			t := time.NewTicker(d)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					f()
				}
			}
		}()
	}
	every(w.cfg.PromoteEvery, func() {
		if _, err := w.queue.PromoteDue(ctx, 500); err != nil && ctx.Err() == nil {
			w.log.Warn("promote retries", "err", err)
		}
	})
	every(w.cfg.Visibility/2, func() {
		free := w.cfg.Concurrency - len(slots)
		if free <= 0 {
			return
		}
		msgs, err := w.queue.Reclaim(ctx, w.cfg.Visibility, free)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("reclaim", "err", err)
			}
			return
		}
		if len(msgs) > 0 {
			metrics.Reclaimed.Add(float64(len(msgs)))
			w.log.Info("reclaimed abandoned deliveries", "count", len(msgs))
			dispatch(msgs)
		}
	})
	every(w.cfg.SweepEvery, func() { w.sweep(ctx) })
	every(5*time.Second, func() { w.recordDepth(ctx) })

	// Fetch loop: only ask for as many messages as there are free slots.
	for ctx.Err() == nil {
		free := w.cfg.Concurrency - len(slots)
		if free == 0 {
			select {
			case slots <- struct{}{}: // wait for a slot, then give it back
				<-slots
			case <-ctx.Done():
			}
			continue
		}
		msgs, err := w.queue.Read(ctx, free, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.log.Warn("read queue", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		dispatch(msgs)
	}

	loops.Wait()
	done := make(chan struct{})
	go func() { inflight.Wait(); close(done) }()
	select {
	case <-done:
		w.log.Info("drained in-flight jobs")
	case <-time.After(drain):
		w.log.Warn("drain deadline reached; unfinished jobs will be reclaimed by other workers")
		cancelProc()
		<-done
	}
	return nil
}

// handle runs one delivery to completion: a terminal state, a scheduled
// retry, or (if the lease was lost) nothing.
func (w *Worker) handle(ctx context.Context, m queue.Message) {
	log := w.log.With("job", m.JobID)
	claim, ok, err := w.store.Start(ctx, m.JobID, w.cfg.Visibility)
	if err != nil {
		log.Warn("start attempt", "err", err) // left pending; it will be reclaimed
		return
	}
	if !ok { // already finished: duplicate delivery
		_ = w.queue.Ack(ctx, m.EntryID)
		return
	}

	began := time.Now()
	stopBeat := w.heartbeat(ctx, m, claim)
	result, cached, err := w.process(ctx, m.JobID, claim)
	stopBeat()

	switch {
	case err == nil:
		err = w.store.Succeed(ctx, m.JobID, claim.LeaseToken, result, cached)
		if errors.Is(err, store.ErrLostLease) {
			metrics.LostLease.Inc()
			log.Info("discarding result: job was reclaimed by another worker")
			_ = w.queue.Ack(ctx, m.EntryID)
			return
		}
		if err != nil {
			log.Warn("record success", "err", err)
			return
		}
		metrics.Attempts.WithLabelValues("succeeded").Observe(time.Since(began).Seconds())
		metrics.Finished.WithLabelValues("succeeded").Inc()
		w.observeEndToEnd(ctx, m.JobID)
		_ = w.queue.Ack(ctx, m.EntryID)

	default:
		var perm PermanentError
		permanent := errors.As(err, &perm)
		status, ferr := w.store.Fail(ctx, m.JobID, claim.LeaseToken, err.Error(), permanent)
		if errors.Is(ferr, store.ErrLostLease) {
			metrics.LostLease.Inc()
			_ = w.queue.Ack(ctx, m.EntryID)
			return
		}
		if ferr != nil {
			log.Warn("record failure", "err", ferr)
			return
		}
		metrics.Attempts.WithLabelValues("failed").Observe(time.Since(began).Seconds())
		if status == store.Dead {
			metrics.Finished.WithLabelValues("dead").Inc()
			log.Warn("job dead", "attempt", claim.Attempt, "err", err)
			_ = w.queue.DeadLetter(ctx, m, err.Error())
			return
		}
		delay := queue.Backoff(claim.Attempt, w.cfg.BackoffBase, w.cfg.BackoffMax, rand.Float64())
		metrics.Retries.Inc()
		log.Info("retry scheduled", "attempt", claim.Attempt, "delay", delay, "err", err)
		_ = w.queue.RetryLater(ctx, m, delay)
	}
}

// heartbeat keeps both the Redis delivery and the Postgres lease alive while
// a long job runs.
func (w *Worker) heartbeat(ctx context.Context, m queue.Message, c store.Claim) (stop func()) {
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.cfg.Visibility / 3)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := w.queue.Heartbeat(hbCtx, m.EntryID); err != nil && hbCtx.Err() == nil {
					w.log.Warn("queue heartbeat", "job", m.JobID, "err", err)
				}
				if err := w.store.Heartbeat(hbCtx, m.JobID, c.LeaseToken, w.cfg.Visibility); err != nil && hbCtx.Err() == nil {
					w.log.Warn("lease heartbeat", "job", m.JobID, "err", err)
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

// process produces the result JSON, from the cache when possible.
func (w *Worker) process(ctx context.Context, jobID string, c store.Claim) (json.RawMessage, bool, error) {
	if w.cfg.BeforeProcess != nil {
		if err := w.cfg.BeforeProcess(ctx, jobID, c.Attempt); err != nil {
			return nil, false, err
		}
	}
	if hit, err := w.cache.Get(ctx, audio.Version, c.InputSHA256); err == nil && hit != nil {
		metrics.CacheHits.WithLabelValues("worker").Inc()
		return hit, true, nil
	}
	if w.cfg.ChaosProcessDelay > 0 {
		select {
		case <-time.After(w.cfg.ChaosProcessDelay):
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	if w.cfg.ChaosFailRate > 0 && rand.Float64() < w.cfg.ChaosFailRate {
		return nil, false, errors.New("chaos: injected transient failure")
	}
	f, err := w.blobs.Open(c.InputSHA256)
	if err != nil {
		return nil, false, fmt.Errorf("open input: %w", err) // transient: shared volume hiccup
	}
	defer f.Close()
	res, err := w.cfg.Analyze(f, audio.DefaultParams)
	if err != nil {
		return nil, false, PermanentError{fmt.Errorf("analyze: %w", err)}
	}
	out, err := json.Marshal(res)
	if err != nil {
		return nil, false, err
	}
	if err := w.cache.Put(ctx, audio.Version, c.InputSHA256, out); err != nil {
		w.log.Warn("cache result", "err", err) // the result is still recorded in Postgres
	}
	return out, false, nil
}

// sweep re-enqueues jobs Postgres says are unfinished but the queue appears
// to have lost (Redis restart, crash between insert and enqueue). Duplicates
// are harmless: Start only lets one attempt hold the lease.
func (w *Worker) sweep(ctx context.Context) {
	ids, err := w.store.Stale(ctx, w.cfg.SweepIdle, 500)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("sweep", "err", err)
		}
		return
	}
	for _, id := range ids {
		if err := w.queue.Enqueue(ctx, id); err != nil {
			w.log.Warn("sweep enqueue", "job", id, "err", err)
			return
		}
	}
	if len(ids) > 0 {
		metrics.Swept.Add(float64(len(ids)))
		w.log.Info("re-enqueued stale jobs", "count", len(ids))
	}
}

func (w *Worker) recordDepth(ctx context.Context) {
	st, err := w.queue.Stats(ctx)
	if err != nil {
		return
	}
	metrics.QueueDepth.WithLabelValues("lag").Set(float64(st.Lag))
	metrics.QueueDepth.WithLabelValues("pending").Set(float64(st.Pending))
	metrics.QueueDepth.WithLabelValues("delayed").Set(float64(st.Delayed))
	metrics.QueueDepth.WithLabelValues("dead").Set(float64(st.Dead))
}

func (w *Worker) observeEndToEnd(ctx context.Context, id string) {
	var created, finished time.Time
	err := w.store.Pool.QueryRow(ctx, `select created_at, finished_at from jobs where id = $1`, id).Scan(&created, &finished)
	if err == nil {
		metrics.EndToEnd.Observe(finished.Sub(created).Seconds())
	}
}
