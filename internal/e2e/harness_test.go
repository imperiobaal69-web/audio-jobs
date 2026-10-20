// Package e2e runs the API and workers in-process against real Redis and
// Postgres. Each test gets its own database and Redis key prefix.
//
// Start the dependencies with `make dev-up` (or set TEST_DATABASE_URL and
// TEST_REDIS_ADDR); the tests skip when they are unreachable, and CI runs
// them against service containers.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/imperiobaal69-web/audio-jobs/internal/api"
	"github.com/imperiobaal69-web/audio-jobs/internal/audio"
	"github.com/imperiobaal69-web/audio-jobs/internal/blob"
	"github.com/imperiobaal69-web/audio-jobs/internal/cache"
	"github.com/imperiobaal69-web/audio-jobs/internal/queue"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
	"github.com/imperiobaal69-web/audio-jobs/internal/worker"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type harness struct {
	t      *testing.T
	ctx    context.Context
	store  *store.Store
	rdb    *redis.Client
	prefix string
	blobs  *blob.Store
	api    *httptest.Server
	log    *slog.Logger
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	adminURL := env("TEST_DATABASE_URL", "postgres://postgres@127.0.0.1:5440/postgres?sslmode=disable")
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Skipf("postgres unavailable (%v); run make dev-up", err)
	}
	dbName := "aj_test_" + randomHex(6)
	if _, err := admin.Exec(ctx, "create database "+dbName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "drop database if exists "+dbName+" with (force)")
		admin.Close(context.Background())
	})
	dbURL := strings.Replace(adminURL, "/postgres?", "/"+dbName+"?", 1)
	st, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx, 0); err != nil {
		t.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: env("TEST_REDIS_ADDR", "127.0.0.1:6390")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable (%v); run make dev-up", err)
	}
	prefix := "ajtest:" + randomHex(6)
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+":*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
		rdb.Close()
	})

	h := &harness{
		t: t, ctx: ctx, store: st, rdb: rdb, prefix: prefix,
		blobs: &blob.Store{Dir: t.TempDir()},
		log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	srv := &api.Server{
		Store: st, Queue: h.queue("api"), Blobs: h.blobs, Cache: h.cache(), Log: h.log,
		MaxUpload: 8 << 20, MaxAttempts: 3,
		RedisPing: func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
	}
	h.api = httptest.NewServer(srv.Handler())
	t.Cleanup(h.api.Close)
	return h
}

func (h *harness) queue(consumer string) *queue.Queue { return queue.New(h.rdb, h.prefix, consumer) }
func (h *harness) cache() *cache.Results             { return cache.New(h.rdb, h.prefix, time.Hour) }

// startWorker runs a worker until the returned stop function is called;
// stop returns once the worker has drained.
func (h *harness) startWorker(name string, cfg worker.Config) (stop func()) {
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 50 * time.Millisecond
	}
	if cfg.Visibility == 0 {
		cfg.Visibility = 3 * time.Second
	}
	cfg.PromoteEvery = 20 * time.Millisecond
	w := worker.New(cfg, h.store, h.queue(name), h.blobs, h.cache(), h.log)
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(ctx, 10*time.Second); err != nil {
			h.t.Errorf("worker %s: %v", name, err)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	h.t.Cleanup(stop)
	return stop
}

type submitResult struct {
	Status int
	Job    store.Job
	Error  string
}

func (h *harness) submit(tenant, idemKey string, body []byte) submitResult {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.api.URL+"/v1/jobs", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenant)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	out := submitResult{Status: resp.StatusCode}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var e map[string]string
		_ = json.Unmarshal(raw, &e)
		out.Error = e["error"]
		return out
	}
	if err := json.Unmarshal(raw, &out.Job); err != nil {
		h.t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

func (h *harness) get(tenant, id string) store.Job {
	h.t.Helper()
	j, err := h.store.Get(h.ctx, tenant, id)
	if err != nil {
		h.t.Fatal(err)
	}
	return j
}

// waitFor polls the job until it reaches a terminal status.
func (h *harness) waitFor(tenant, id string, timeout time.Duration) store.Job {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		j := h.get(tenant, id)
		if j.Status.Terminal() {
			return j
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("job %s still %s after %s (attempts=%d, last_error=%v)", id, j.Status, timeout, j.Attempts, deref(j.LastError))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func wav(seed uint64, ms int) []byte {
	return audio.Synthesize(audio.SynthSpec{Seed: seed, SampleRate: 16000, Channels: 1, DurationMs: ms, PauseEvery: 1500, PauseMs: 500})
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func decodeResult(t *testing.T, j store.Job) audio.Result {
	t.Helper()
	var r audio.Result
	if err := json.Unmarshal(j.Result, &r); err != nil {
		t.Fatalf("result %s: %v", j.Result, err)
	}
	return r
}
