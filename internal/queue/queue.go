// Package queue is an at-least-once job queue on Redis Streams.
//
//   - Ready jobs are entries in a stream read by a consumer group.
//   - A delivered entry stays in the group's pending list until acknowledged;
//     if its consumer dies, another consumer reclaims it once it has been idle
//     longer than the visibility timeout (XAUTOCLAIM). Long jobs heartbeat to
//     reset their idle time.
//   - Retries wait in a sorted set scored by due time; a Lua script moves due
//     entries back to the stream atomically, so concurrent promoters never
//     duplicate them.
//   - Permanently failed jobs are copied to a dead-letter stream.
//
// The queue only carries job IDs. Job state lives in Postgres, which is what
// makes duplicate deliveries harmless.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message is one delivery of a job.
type Message struct {
	EntryID string // stream entry ID, used to ack / heartbeat
	JobID   string
}

// Queue is bound to a key prefix and a consumer group.
type Queue struct {
	rdb      *redis.Client
	stream   string
	delayed  string
	dead     string
	group    string
	maxLen   int64
	promote  *redis.Script
	consumer string
}

// New returns a queue using keys "<prefix>:stream", "<prefix>:delayed" and
// "<prefix>:dead". consumer must be unique per process (e.g. the pod name).
func New(rdb *redis.Client, prefix, consumer string) *Queue {
	return &Queue{
		rdb:      rdb,
		stream:   prefix + ":stream",
		delayed:  prefix + ":delayed",
		dead:     prefix + ":dead",
		group:    "workers",
		maxLen:   1_000_000,
		consumer: consumer,
		// KEYS[1]=delayed zset, KEYS[2]=stream; ARGV[1]=now(ms), ARGV[2]=limit
		promote: redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job in ipairs(due) do
  redis.call('ZREM', KEYS[1], job)
  redis.call('XADD', KEYS[2], '*', 'job_id', job)
end
return #due`),
	}
}

// Init creates the stream and consumer group if missing.
func (q *Queue) Init(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Enqueue makes a job ready now.
func (q *Queue) Enqueue(ctx context.Context, jobID string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream, MaxLen: q.maxLen, Approx: true, Values: []string{"job_id", jobID},
	}).Err()
}

// Read blocks up to `block` for at most `count` new deliveries.
func (q *Queue) Read(ctx context.Context, count int, block time.Duration) ([]Message, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: q.group, Consumer: q.consumer, Streams: []string{q.stream, ">"},
		Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, s := range res {
		out = append(out, toMessages(s.Messages)...)
	}
	return out, nil
}

// Reclaim takes over deliveries that have been pending (unacknowledged) for
// longer than minIdle, i.e. whose consumer probably crashed.
func (q *Queue) Reclaim(ctx context.Context, minIdle time.Duration, count int) ([]Message, error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: q.stream, Group: q.group, Consumer: q.consumer,
		MinIdle: minIdle, Start: "0-0", Count: int64(count),
	}).Result()
	if err != nil {
		return nil, err
	}
	return toMessages(msgs), nil
}

// Heartbeat resets the idle time of a delivery this consumer still owns, so
// that a long-running job is not reclaimed while it is being processed.
func (q *Queue) Heartbeat(ctx context.Context, entryID string) error {
	return q.rdb.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream: q.stream, Group: q.group, Consumer: q.consumer, MinIdle: 0, Messages: []string{entryID},
	}).Err()
}

// Ack acknowledges and deletes a delivery.
func (q *Queue) Ack(ctx context.Context, entryID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.XAck(ctx, q.stream, q.group, entryID)
	pipe.XDel(ctx, q.stream, entryID)
	_, err := pipe.Exec(ctx)
	return err
}

// RetryLater acknowledges a delivery and schedules the job again after delay.
func (q *Queue) RetryLater(ctx context.Context, m Message, delay time.Duration) error {
	due := float64(time.Now().Add(delay).UnixMilli())
	pipe := q.rdb.TxPipeline()
	pipe.ZAdd(ctx, q.delayed, redis.Z{Score: due, Member: m.JobID})
	pipe.XAck(ctx, q.stream, q.group, m.EntryID)
	pipe.XDel(ctx, q.stream, m.EntryID)
	_, err := pipe.Exec(ctx)
	return err
}

// DeadLetter acknowledges a delivery and records it in the dead-letter stream.
func (q *Queue) DeadLetter(ctx context.Context, m Message, reason string) error {
	pipe := q.rdb.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{Stream: q.dead, MaxLen: 100_000, Approx: true,
		Values: []string{"job_id", m.JobID, "reason", reason}})
	pipe.XAck(ctx, q.stream, q.group, m.EntryID)
	pipe.XDel(ctx, q.stream, m.EntryID)
	_, err := pipe.Exec(ctx)
	return err
}

// PromoteDue moves retries whose time has come back into the stream.
func (q *Queue) PromoteDue(ctx context.Context, limit int) (int, error) {
	n, err := q.promote.Run(ctx, q.rdb, []string{q.delayed, q.stream},
		strconv.FormatInt(time.Now().UnixMilli(), 10), limit).Int()
	return n, err
}

// Stats for metrics and tests.
type Stats struct {
	Lag     int64 // entries not yet delivered to the group
	Pending int64 // delivered, not acknowledged
	Delayed int64 // waiting for a retry
	Dead    int64 // entries in the dead-letter stream
}

func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	groups, err := q.rdb.XInfoGroups(ctx, q.stream).Result()
	if err != nil {
		return st, err
	}
	for _, g := range groups {
		if g.Name == q.group {
			st.Lag, st.Pending = g.Lag, g.Pending
		}
	}
	if st.Delayed, err = q.rdb.ZCard(ctx, q.delayed).Result(); err != nil {
		return st, err
	}
	st.Dead, err = q.rdb.XLen(ctx, q.dead).Result()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	return st, err
}

func toMessages(entries []redis.XMessage) []Message {
	out := make([]Message, 0, len(entries))
	for _, e := range entries {
		id, _ := e.Values["job_id"].(string)
		out = append(out, Message{EntryID: e.ID, JobID: id})
	}
	return out
}

// Backoff returns the retry delay for an attempt: base * 2^(attempt-1),
// capped, with up to 20% jitter supplied by the caller (0..1).
func Backoff(attempt int, base, max time.Duration, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base << (attempt - 1)
	if d > max || d <= 0 {
		d = max
	}
	return d + time.Duration(float64(d)*0.2*jitter)
}

// String for logs.
func (m Message) String() string { return fmt.Sprintf("job %s (entry %s)", m.JobID, m.EntryID) }
