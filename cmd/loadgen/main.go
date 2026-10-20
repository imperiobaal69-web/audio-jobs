// Command loadgen submits synthetic recordings to the API and reports
// throughput, latency percentiles and result integrity.
//
// End-to-end latency uses the server's created_at / finished_at, so the
// client's polling interval does not distort it. Every succeeded job's result
// is checked against the duration of the recording that was submitted.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imperiobaal69-web/audio-jobs/internal/audio"
)

type job struct {
	ID         string          `json:"id"`
	Status     string          `json:"status"`
	Cached     bool            `json:"cached"`
	Attempts   int             `json:"attempts"`
	Result     json.RawMessage `json:"result"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at"`
}

type outcome struct {
	submit     time.Duration
	endToEnd   time.Duration
	status     string
	cached     bool
	attempts   int
	integrity  bool
	submitCode int
}

type report struct {
	Label            string  `json:"label"`
	Jobs             int     `json:"jobs"`
	Succeeded        int     `json:"succeeded"`
	Dead             int     `json:"dead"`
	SubmitErrors     int     `json:"submit_errors"`
	TimedOut         int     `json:"timed_out"`
	Cached           int     `json:"cached"`
	Retried          int     `json:"retried"`
	IntegrityFailed  int     `json:"integrity_failed"`
	WallSeconds      float64 `json:"wall_seconds"`
	ThroughputPerSec float64 `json:"throughput_jobs_per_sec"`
	SubmitP50Ms      float64 `json:"submit_p50_ms"`
	SubmitP99Ms      float64 `json:"submit_p99_ms"`
	EndToEndP50Ms    float64 `json:"e2e_p50_ms"`
	EndToEndP95Ms    float64 `json:"e2e_p95_ms"`
	EndToEndP99Ms    float64 `json:"e2e_p99_ms"`
}

func main() {
	apiURL := flag.String("api", "http://127.0.0.1:8080", "API base URL")
	jobs := flag.Int("jobs", 500, "total submissions")
	concurrency := flag.Int("concurrency", 16, "concurrent clients")
	rate := flag.Float64("rate", 0, "submissions per second across all clients (0 = unthrottled)")
	distinct := flag.Int("distinct", 0, "distinct recordings (0 = every job unique); fewer means cache hits")
	durationMs := flag.Int("duration-ms", 30_000, "length of each synthetic recording")
	tenants := flag.Int("tenants", 8, "tenants to spread jobs over")
	timeout := flag.Duration("timeout", 5*time.Minute, "per-job completion timeout")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "seed for recordings (different runs do not hit each other's cache)")
	label := flag.String("label", "", "label for the report")
	out := flag.String("out", "", "write the JSON report here as well")
	flag.Parse()

	n := *distinct
	if n <= 0 || n > *jobs {
		n = *jobs
	}
	fmt.Fprintf(os.Stderr, "rendering %d recordings of %d ms...\n", n, *durationMs)
	bodies := make([][]byte, n)
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bodies[i] = audio.Synthesize(audio.SynthSpec{Seed: *seed + uint64(i), SampleRate: 16000, Channels: 1,
				DurationMs: *durationMs, PauseEvery: 3000, PauseMs: 800})
		}(i)
	}
	wg.Wait()

	client := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency * 2}}
	var limiter <-chan time.Time
	if *rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / *rate))
		defer t.Stop()
		limiter = t.C
	}
	work := make(chan int)
	results := make([]outcome, *jobs)
	var done atomic.Int64
	start := time.Now()
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				results[i] = runOne(client, *apiURL, fmt.Sprintf("tenant-%d", i%*tenants), bodies[i%n], int64(*durationMs), *timeout)
				if d := done.Add(1); d%100 == 0 {
					fmt.Fprintf(os.Stderr, "  %d/%d done\n", d, *jobs)
				}
			}
		}()
	}
	for i := 0; i < *jobs; i++ {
		if limiter != nil {
			<-limiter
		}
		work <- i
	}
	close(work)
	wg.Wait()
	wall := time.Since(start)

	rep := summarize(*label, results, wall)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(rep)
	if *out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		_ = os.WriteFile(*out, append(b, '\n'), 0o644)
	}
	if rep.Succeeded != rep.Jobs || rep.IntegrityFailed > 0 {
		os.Exit(1)
	}
}

func runOne(c *http.Client, base, tenant string, body []byte, wantMs int64, timeout time.Duration) outcome {
	var o outcome
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/v1/jobs", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenant)
	req.Header.Set("Content-Type", "audio/wav")
	t0 := time.Now()
	resp, err := c.Do(req)
	o.submit = time.Since(t0)
	if err != nil {
		o.status = "submit_error"
		return o
	}
	o.submitCode = resp.StatusCode
	var j job
	err = json.NewDecoder(resp.Body).Decode(&j)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode >= 300 {
		o.status = "submit_error"
		return o
	}

	deadline := time.Now().Add(timeout)
	poll := 50 * time.Millisecond
	for j.Status != "succeeded" && j.Status != "dead" {
		if time.Now().After(deadline) {
			o.status = "timeout"
			return o
		}
		time.Sleep(poll)
		if poll < time.Second {
			poll += poll / 4
		}
		greq, _ := http.NewRequest(http.MethodGet, base+"/v1/jobs/"+j.ID, nil)
		greq.Header.Set("X-Tenant-ID", tenant)
		gresp, err := c.Do(greq)
		if err != nil {
			continue
		}
		_ = json.NewDecoder(gresp.Body).Decode(&j)
		gresp.Body.Close()
	}
	o.status, o.cached, o.attempts = j.Status, j.Cached, j.Attempts
	if j.FinishedAt != nil {
		o.endToEnd = j.FinishedAt.Sub(j.CreatedAt)
	}
	if j.Status == "succeeded" {
		var r audio.Result
		o.integrity = json.Unmarshal(j.Result, &r) == nil && r.DurationMs == wantMs && len(r.Waveform) > 0
	}
	return o
}

func summarize(label string, rs []outcome, wall time.Duration) report {
	rep := report{Label: label, Jobs: len(rs), WallSeconds: wall.Seconds()}
	var submits, e2e []float64
	for _, r := range rs {
		switch r.status {
		case "succeeded":
			rep.Succeeded++
			if !r.integrity {
				rep.IntegrityFailed++
			}
			e2e = append(e2e, ms(r.endToEnd))
		case "dead":
			rep.Dead++
		case "timeout":
			rep.TimedOut++
		default:
			rep.SubmitErrors++
		}
		if r.cached {
			rep.Cached++
		}
		if r.attempts > 1 {
			rep.Retried++
		}
		if r.submitCode > 0 {
			submits = append(submits, ms(r.submit))
		}
	}
	rep.ThroughputPerSec = float64(rep.Succeeded) / wall.Seconds()
	rep.SubmitP50Ms, rep.SubmitP99Ms = pct(submits, 50), pct(submits, 99)
	rep.EndToEndP50Ms, rep.EndToEndP95Ms, rep.EndToEndP99Ms = pct(e2e, 50), pct(e2e, 95), pct(e2e, 99)
	return rep
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := int(float64(len(s)-1) * p / 100)
	return float64(int(s[idx]*10)) / 10
}
