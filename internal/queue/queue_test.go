package queue

import (
	"testing"
	"time"
)

func TestBackoffDoublesAndCaps(t *testing.T) {
	base, max := time.Second, 30*time.Second
	got := []time.Duration{}
	for attempt := 1; attempt <= 7; attempt++ {
		got = append(got, Backoff(attempt, base, max, 0))
	}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("attempt %d: %s, want %s", i+1, got[i], want[i]*time.Second)
		}
	}
	if d := Backoff(1, base, max, 1); d != 1200*time.Millisecond {
		t.Fatalf("full jitter: %s", d)
	}
	if d := Backoff(80, base, max, 0); d != max { // shift overflow must not wrap
		t.Fatalf("overflow: %s", d)
	}
}
