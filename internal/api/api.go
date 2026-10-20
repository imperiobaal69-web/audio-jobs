// Package api is the HTTP interface: submit a recording, poll a job, list a
// tenant's jobs.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/imperiobaal69-web/audio-jobs/internal/audio"
	"github.com/imperiobaal69-web/audio-jobs/internal/blob"
	"github.com/imperiobaal69-web/audio-jobs/internal/cache"
	"github.com/imperiobaal69-web/audio-jobs/internal/metrics"
	"github.com/imperiobaal69-web/audio-jobs/internal/queue"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
)

type Server struct {
	Store       *store.Store
	Queue       *queue.Queue
	Blobs       *blob.Store
	Cache       *cache.Results
	Log         *slog.Logger
	MaxUpload   int64 // bytes
	MaxAttempts int
	RedisPing   func(context.Context) error
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.submit)
	mux.HandleFunc("GET /v1/jobs/{id}", s.get)
	mux.HandleFunc("GET /v1/jobs", s.list)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	tenant := r.Header.Get("X-Tenant-ID")
	if tenant == "" {
		writeError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}
	idemKey := r.Header.Get("Idempotency-Key")
	if len(idemKey) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key too long")
		return
	}

	body := http.MaxBytesReader(w, r.Body, s.MaxUpload)
	sum, size, err := s.Blobs.Put(body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload exceeds %d bytes", s.MaxUpload))
			return
		}
		s.fail(w, "store upload", err)
		return
	}
	if err := s.validateWAV(sum); err != nil {
		writeError(w, http.StatusUnsupportedMediaType, err.Error())
		return
	}

	ctx := r.Context()
	cached, err := s.Cache.Get(ctx, audio.Version, sum)
	if err != nil {
		s.Log.Warn("cache lookup", "err", err) // degrade to processing
		cached = nil
	}
	job, created, err := s.Store.Create(ctx, store.NewJob{
		TenantID: tenant, IdempotencyKey: idemKey, InputSHA256: sum, InputBytes: size,
		MaxAttempts: s.MaxAttempts, CachedResult: cached,
	})
	if err != nil {
		s.fail(w, "create job", err)
		return
	}
	switch {
	case !created:
		metrics.Submitted.WithLabelValues("duplicate").Inc()
		writeJSON(w, http.StatusOK, job)
		return
	case cached != nil:
		metrics.Submitted.WithLabelValues("cached").Inc()
		metrics.CacheHits.WithLabelValues("api").Inc()
		writeJSON(w, http.StatusCreated, job)
		return
	}
	// If this enqueue fails (or the process dies right here), the job stays
	// 'queued' in Postgres and the sweeper enqueues it later.
	if err := s.Queue.Enqueue(ctx, job.ID); err != nil {
		s.Log.Warn("enqueue failed; sweeper will retry", "job", job.ID, "err", err)
	}
	metrics.Submitted.WithLabelValues("queued").Inc()
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) validateWAV(sum string) error {
	f, err := s.Blobs.Open(sum)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := audio.ParseHeader(f); err != nil {
		return fmt.Errorf("not a supported WAV file: %v", err)
	}
	return nil
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	tenant := r.Header.Get("X-Tenant-ID")
	job, err := s.Store.Get(r.Context(), tenant, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.fail(w, "get job", err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

type listResponse struct {
	Jobs       []store.Job `json:"jobs"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	tenant := r.Header.Get("X-Tenant-ID")
	if tenant == "" {
		writeError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be 1..500")
			return
		}
		limit = n
	}
	status := store.Status(q.Get("status"))
	switch status {
	case "", store.Queued, store.Running, store.Succeeded, store.Failed, store.Dead:
	default:
		writeError(w, http.StatusBadRequest, "unknown status")
		return
	}
	var after *store.Cursor
	if c := q.Get("cursor"); c != "" {
		cur, err := decodeCursor(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad cursor")
			return
		}
		after = &cur
	}
	jobs, err := s.Store.List(r.Context(), tenant, status, after, limit)
	if err != nil {
		s.fail(w, "list jobs", err)
		return
	}
	resp := listResponse{Jobs: jobs}
	if resp.Jobs == nil {
		resp.Jobs = []store.Job{}
	}
	if len(jobs) == limit {
		last := jobs[len(jobs)-1]
		resp.NextCursor = encodeCursor(store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres: "+err.Error())
		return
	}
	if err := s.RedisPing(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func encodeCursor(c store.Cursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

func decodeCursor(s string) (store.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return store.Cursor{}, err
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return store.Cursor{}, errors.New("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	return store.Cursor{CreatedAt: t, ID: id}, err
}

func (s *Server) fail(w http.ResponseWriter, what string, err error) {
	s.Log.Error(what, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
