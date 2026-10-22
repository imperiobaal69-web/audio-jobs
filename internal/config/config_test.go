package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"HTTP_ADDR", "MAX_ATTEMPTS", "VISIBILITY_TIMEOUT", "CHAOS_FAIL_RATE", "ANALYZER"} {
		t.Setenv(k, "")
	}
	c := Load()
	if c.HTTPAddr != ":8080" || c.MaxAttempts != 5 || c.Visibility != 30*time.Second ||
		c.ChaosFailRate != 0 || c.Analyzer != "streaming" {
		t.Fatalf("defaults: %+v", c)
	}
	if c.ConsumerName == "" {
		t.Fatal("consumer name must default to host and pid")
	}
}

func TestLoadReadsTheEnvironment(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("MAX_UPLOAD_BYTES", "1048576")
	t.Setenv("MAX_ATTEMPTS", "8")
	t.Setenv("VISIBILITY_TIMEOUT", "90s")
	t.Setenv("CHAOS_FAIL_RATE", "0.25")
	t.Setenv("CONSUMER_NAME", "worker-0")
	c := Load()
	if c.HTTPAddr != ":9000" || c.MaxUploadBytes != 1<<20 || c.MaxAttempts != 8 ||
		c.Visibility != 90*time.Second || c.ChaosFailRate != 0.25 || c.ConsumerName != "worker-0" {
		t.Fatalf("from env: %+v", c)
	}
}

func TestLoadFallsBackOnMalformedValues(t *testing.T) {
	t.Setenv("MAX_ATTEMPTS", "five")
	t.Setenv("VISIBILITY_TIMEOUT", "30") // a duration needs a unit
	c := Load()
	if c.MaxAttempts != 5 || c.Visibility != 30*time.Second {
		t.Fatalf("malformed values: attempts %d, visibility %s", c.MaxAttempts, c.Visibility)
	}
}
