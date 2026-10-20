// Command worker processes jobs. It serves /healthz and /metrics on
// METRICS_ADDR.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/imperiobaal69-web/audio-jobs/internal/blob"
	"github.com/imperiobaal69-web/audio-jobs/internal/cache"
	"github.com/imperiobaal69-web/audio-jobs/internal/config"
	"github.com/imperiobaal69-web/audio-jobs/internal/queue"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
	"github.com/imperiobaal69-web/audio-jobs/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("consumer", config.Load().ConsumerName)
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = metricsSrv.ListenAndServe() }()

	w := worker.New(worker.Config{
		Concurrency: cfg.Concurrency, Visibility: cfg.Visibility,
		BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax,
		SweepEvery: cfg.SweepEvery, SweepIdle: cfg.SweepIdle,
		ChaosFailRate: cfg.ChaosFailRate, ChaosProcessDelay: cfg.ChaosProcessDelay,
	}, st, queue.New(rdb, cfg.KeyPrefix, cfg.ConsumerName), &blob.Store{Dir: cfg.BlobDir},
		cache.New(rdb, cfg.KeyPrefix, cfg.CacheTTL), log)

	log.Info("worker started", "concurrency", cfg.Concurrency)
	if err := w.Run(ctx, cfg.ShutdownGrace); err != nil {
		log.Error("worker", "err", err)
		os.Exit(1)
	}
	_ = metricsSrv.Close()
	log.Info("worker stopped")
}
