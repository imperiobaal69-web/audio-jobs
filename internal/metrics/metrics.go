// Package metrics defines the Prometheus metrics shared by the API and workers.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Submitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aj_jobs_submitted_total", Help: "Job submissions by outcome (queued, cached, duplicate).",
	}, []string{"outcome"})
	Finished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aj_jobs_finished_total", Help: "Jobs reaching a terminal state (succeeded, dead).",
	}, []string{"status"})
	Attempts = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "aj_attempt_duration_seconds", Help: "Duration of one processing attempt by outcome.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	}, []string{"outcome"})
	EndToEnd = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "aj_job_end_to_end_seconds", Help: "Time from submission to success.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	})
	Retries   = promauto.NewCounter(prometheus.CounterOpts{Name: "aj_retries_total", Help: "Attempts scheduled for retry."})
	Reclaimed = promauto.NewCounter(prometheus.CounterOpts{Name: "aj_reclaimed_total", Help: "Deliveries reclaimed from dead consumers."})
	Swept     = promauto.NewCounter(prometheus.CounterOpts{Name: "aj_swept_total", Help: "Jobs re-enqueued from Postgres by the sweeper."})
	LostLease = promauto.NewCounter(prometheus.CounterOpts{Name: "aj_lost_lease_total", Help: "Attempts discarded because another worker took over."})
	CacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aj_cache_hits_total", Help: "Result cache hits by where they happened (api, worker).",
	}, []string{"where"})
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aj_queue_depth", Help: "Queue depth by state (lag, pending, delayed, dead).",
	}, []string{"state"})
	InFlight = promauto.NewGauge(prometheus.GaugeOpts{Name: "aj_worker_in_flight", Help: "Jobs being processed by this worker."})
)
