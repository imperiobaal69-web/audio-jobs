# Case study: the chaos test that passed for the wrong reason

This is the hardest problem I hit while building this system: what I saw,
how I diagnosed it, the options I weighed, what I changed, and what the
evidence looked like afterwards. Every number here comes from the scripts in
this repository; the commits are referenced so the before and after states
can be checked out and re-run.

## Context

The system's reliability claim is simple: **once the API accepts a
recording, the job finishes**, no matter which process dies. Postgres is the
source of truth for job state; Redis Streams only carries deliveries. Three
mechanisms back the claim:

1. Reclaim. A delivery stays in the consumer group's pending list until
   acknowledged. If a worker dies, another one takes it over with `XAUTOCLAIM`
   once it has been idle longer than the visibility timeout.
2. Lease fencing. Each attempt holds a lease token in Postgres. A worker
   whose lease was taken over cannot overwrite the newer attempt's result.
3. Sweeper. If Redis loses data, workers find unfinished jobs in Postgres
   that have not been touched for a while and enqueue them again.

The integration tests covered each mechanism in isolation. I wanted
evidence that they hold together under load, so I wrote
[`scripts/chaos.sh`](../scripts/chaos.sh). It submits 1,000 jobs at 40 per
second while it:

- SIGKILLs a random worker every 2 seconds and restarts it under a new
  consumer name;
- makes 5% of attempts fail with an injected transient error;
- restarts Redis without persistence after 10 seconds.

It passes only if every job succeeds with a correct result.

## Symptom: a pass that did not add up

The first run passed: 1,000 of 1,000 jobs succeeded. But two numbers in the
report did not fit the story. The sweeper had re-enqueued **0** jobs, and
the API had failed **0** enqueues, even though Redis had been wiped in the
middle of the run.

I checked that the restart had happened at all. It had: Redis uptime matched
the restart time, and one worker log contained
`NOGROUP No such key 'aj:stream' or consumer group 'workers'`.

## Diagnosis

The consumer group is created when a worker starts (`Queue.Init`). After a
Redis restart without persistence the group no longer exists:

- The API's next `XADD` silently re-creates the **stream**, but not the
  group.
- Every `XREADGROUP` from a running worker then fails with `NOGROUP`.
- The worker's fetch loop treated that like any transient error: log, sleep
  500 ms, try again, forever.

So why did the test pass? Because the chaos loop itself was restarting a
worker every 2 seconds, and each restarted worker ran `Init` and re-created
the group. **The fault injection was repairing the fault it was meant to
expose.** In production, workers run for days; nothing would have restarted
them.

To prove this rather than argue it, I added `KILL_WORKERS=0` to the script:
restart Redis, but leave the workers alone.

| run (worker code at commit `20cf7ed`) | jobs succeeded | stuck in `queued` | `NOGROUP` errors per worker |
|---|---|---|---|
| Redis restart, no worker kills | 379 / 600 | 220 | about 311 |

Every worker sat in a tight error loop while a third of the jobs waited
forever. Even the sweeper could not help: it re-enqueued jobs into a stream
that no consumer group could read.

## Options considered

1. Turn on Redis persistence (AOF) and treat a restart as rare. This
   reduces data loss, but managed Redis fails over, gets flushed by mistake,
   or loses its last second of writes. The system would still have no path
   back from a lost group, so this hides the bug instead of fixing it.
2. **Health-check the group and crash the worker on `NOGROUP`, letting
   Kubernetes restart it.** This works, but turns a recoverable condition into
   a restart storm across every pod, and it is exactly the accidental repair
   the test had been relying on.
3. Re-create the group where the error appears. When `XREADGROUP` or
   `XAUTOCLAIM` returns `NOGROUP`, call `XGROUP CREATE ... MKSTREAM` from ID
   `0` and retry once. Starting from `0` matters: entries the API added after
   the restart, before any group existed, are delivered instead of skipped.
   This is local and idempotent (`BUSYGROUP` is ignored when another worker
   got there first), and it adds no new moving parts.

I chose option 3 ([`internal/queue/queue.go`](../internal/queue/queue.go),
`isNoGroup` in `Read` and `Reclaim`). Persistence is still worth enabling in
production, but correctness no longer depends on it.

## A second defect found while making the test harder

The post-fix runs passed, but the sweeper still reported 0 re-enqueues. The
cause was that the workers were keeping up: with 150 ms per attempt and 16
slots, capacity was about 100 jobs per second against 40 arriving. The queue
was nearly empty when Redis restarted, so there was nothing to lose and the
sweeper had nothing to recover.

I raised the injected processing time to 500 ms, putting capacity at 32 jobs
per second, below the arrival rate. A real backlog then builds and the
restart destroys queued work.

Reasoning through what the sweeper would do under a backlog exposed a design
flaw. The sweeper re-enqueues any `queued` job idle longer than `SWEEP_IDLE`,
including jobs that are simply waiting in a long queue, so duplicates are
expected. That was fine for queued jobs. But `Store.Start` allowed a
delivery to take over **any** `running` job, including one whose lease was
still live. A duplicate delivery could therefore steal the lease from a
healthy attempt. Fencing kept the result correct, but the work was done
twice. The same thing could happen when a reclaim fired a few milliseconds
before a lease expired.

The fix makes `Start` return one of three outcomes instead of a boolean:

- `Started`: the job is queued, failed, or running with an *expired*
  lease, and this delivery now holds the lease;
- `AlreadyDone`: the job is terminal, so the delivery is acknowledged;
- `Busy`: another attempt holds a live lease. The delivery is *left
  pending*: it comes back after the visibility timeout, is acknowledged once
  the job is done, or takes over if the other attempt really died.

## Result

Both fixes are in commit `f26d889`.

| scenario ([`docs/results`](results/)) | jobs succeeded | rebuilt from Postgres by the sweeper | abandoned deliveries reclaimed |
|---|---|---|---|
| Redis restart under backlog, no worker kills ([report](results/chaos-redis-restart-only.md)) | 1000 / 1000 | 65 | 0 |
| Full chaos: 18 SIGKILLs + Redis restart + 5% failures ([report](results/chaos.md)) | 1000 / 1000 | 121 | 40 |

Every result passed the integrity check (the duration in each result matches
the recording that was submitted).

Two regression tests pin the behaviour so it does not depend on someone
re-running the chaos script:

- `TestWorkerRecoversWhenRedisLosesTheConsumerGroup` deletes the stream under
  a running worker and requires the next job to complete. I confirmed it fails
  on the old code ("still queued after 10s") and passes on the new.
- `TestStaleLeaseCannotOverwriteNewerAttempt` now also asserts that a live
  lease returns `Busy` and an expired lease can be taken over.

## What I took from it

- A passing fault-injection test needs its own sanity checks. The report
  included counters for every recovery path, and a zero where I expected
  activity was the only sign that anything was wrong. A test that prints only
  PASS or FAIL would have hidden this.
- Make sure the fault is not repaired by the harness. The kill loop and
  the Redis restart interacted. Running each fault on its own (here,
  `KILL_WORKERS=0`) separated them.
- Saturate the system before injecting failures. A queue that is empty at
  the moment of the fault has nothing to lose. Running below capacity made
  the recovery paths look idle rather than proven.

---

## Shorter case study: profiling the analyzer

The first analyzer decoded the whole file into a growing `[]float64` and
computed squares with `math.Pow(s, 2)`.

- Profile (`go test -cpuprofile`): `math.Pow` and its helpers
  (`frexp`, `ldexp`) took **61%** of CPU. The memory profile showed **414 MB**
  allocated per 10-minute recording.
- Change: stream the samples from a 64 KB buffer into running
  accumulators (peak, sum of squares, current waveform bucket, current
  silence window) and square with `s*s`. Memory no longer grows with the
  length of the recording.
- Correctness: the first implementation stays as `AnalyzeReference`.
  `TestAnalyzeMatchesReference` requires the two to agree on mono, stereo,
  44.1 kHz and a file with fewer samples than waveform buckets.
- Result ([benchmark](results/audio-benchmark.md)): about **20× faster**
  and **3,000× less memory** per recording. End to end
  ([load test](results/load-test.md), identical inputs):
  - throughput rose from 162 to 532 jobs/s;
  - p95 latency fell from 249 ms to 15 ms;
  - peak worker memory fell from 878 MB to 94 MB.

One measurement lesson. The first baseline I recorded, in the commit message
of `a965e38`, was about **1.0 s** per recording. It came from
single-iteration benchmark runs that included first-touch page faults on the
414 MB allocation. With repeated iterations the same code runs in about
0.3 s, and the numbers above use that steadier figure.
