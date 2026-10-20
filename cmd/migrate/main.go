// Command migrate applies the database migrations (run as a Kubernetes Job
// before rolling out new API / worker pods).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/imperiobaal69-web/audio-jobs/internal/config"
	"github.com/imperiobaal69-web/audio-jobs/internal/store"
)

func main() {
	upTo := flag.Int("up-to", 0, "apply migrations up to this version (0 = all)")
	flag.Parse()
	ctx := context.Background()
	st, err := store.Open(ctx, config.Load().DatabaseURL)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := st.Migrate(ctx, *upTo); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")
}
