// Package config reads settings from the environment (12-factor), so the same
// binaries run locally, in CI and in Kubernetes.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr          string
	MetricsAddr       string
	DatabaseURL       string
	RedisAddr         string
	KeyPrefix         string
	BlobDir           string
	MaxUploadBytes    int64
	MaxAttempts       int
	Concurrency       int
	Visibility        time.Duration
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	SweepEvery        time.Duration
	SweepIdle         time.Duration
	CacheTTL          time.Duration
	ShutdownGrace     time.Duration
	ConsumerName      string
	ChaosFailRate     float64
	ChaosProcessDelay time.Duration
}

func Load() Config {
	host, _ := os.Hostname()
	return Config{
		HTTPAddr:          str("HTTP_ADDR", ":8080"),
		MetricsAddr:       str("METRICS_ADDR", ":9090"),
		DatabaseURL:       str("DATABASE_URL", "postgres://postgres@127.0.0.1:5440/audiojobs?sslmode=disable"),
		RedisAddr:         str("REDIS_ADDR", "127.0.0.1:6390"),
		KeyPrefix:         str("KEY_PREFIX", "aj"),
		BlobDir:           str("BLOB_DIR", ".local/blobs"),
		MaxUploadBytes:    int64(num("MAX_UPLOAD_BYTES", 512<<20)),
		MaxAttempts:       int(num("MAX_ATTEMPTS", 5)),
		Concurrency:       int(num("WORKER_CONCURRENCY", 4)),
		Visibility:        dur("VISIBILITY_TIMEOUT", 30*time.Second),
		BackoffBase:       dur("BACKOFF_BASE", time.Second),
		BackoffMax:        dur("BACKOFF_MAX", 5*time.Minute),
		SweepEvery:        dur("SWEEP_EVERY", 30*time.Second),
		SweepIdle:         dur("SWEEP_IDLE", 2*time.Minute),
		CacheTTL:          dur("CACHE_TTL", 7*24*time.Hour),
		ShutdownGrace:     dur("SHUTDOWN_GRACE", 25*time.Second),
		ConsumerName:      str("CONSUMER_NAME", host+"-"+strconv.Itoa(os.Getpid())),
		ChaosFailRate:     num("CHAOS_FAIL_RATE", 0),
		ChaosProcessDelay: dur("CHAOS_PROCESS_DELAY", 0),
	}
}

func str(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func num(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			slog.Error("bad numeric env var, using default", "key", k, "value", v)
			return def
		}
		return f
	}
	return def
}

func dur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("bad duration env var, using default", "key", k, "value", v)
			return def
		}
		return d
	}
	return def
}
