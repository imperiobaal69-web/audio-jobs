// Command api serves the HTTP API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/imperiobaal69-web/audio-jobs/internal/api"
	"github.com/imperiobaal69-web/audio-jobs/internal/blob"
	"github.com/imperiobaal69-web/audio-jobs/internal/cache"
	"github.com/imperiobaal69-web/audio-jobs/internal/config"
	"github.com/imperiobaal69-web/audio-jobs/internal/queue"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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
	q := queue.New(rdb, cfg.KeyPrefix, cfg.ConsumerName)
	if err := q.Init(ctx); err != nil {
		log.Error("redis", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: (&api.Server{
			Store: st, Queue: q, Blobs: &blob.Store{Dir: cfg.BlobDir},
			Cache: cache.New(rdb, cfg.KeyPrefix, cfg.CacheTTL), Log: log,
			MaxUpload: cfg.MaxUploadBytes, MaxAttempts: cfg.MaxAttempts,
			RedisPing: func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
