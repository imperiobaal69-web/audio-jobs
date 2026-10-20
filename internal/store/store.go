// Package store persists jobs in Postgres. Every state transition is a
// single conditional UPDATE, and running jobs carry a lease token so that a
// worker that lost its lease (another worker reclaimed the job) cannot
// overwrite the newer attempt's outcome.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Status of a job.
type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed" // failed an attempt, waiting for a retry
	Dead      Status = "dead"   // permanently failed
)

// Terminal reports whether no further processing will happen.
func (s Status) Terminal() bool { return s == Succeeded || s == Dead }

// Job is one row of the jobs table.
type Job struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Status         Status          `json:"status"`
	InputSHA256    string          `json:"input_sha256"`
	InputBytes     int64           `json:"input_bytes"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LastError      *string         `json:"last_error,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Cached         bool            `json:"cached"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

const jobColumns = `id, tenant_id, idempotency_key, status, input_sha256, input_bytes, attempts, max_attempts,
	last_error, result, cached, created_at, updated_at, started_at, finished_at`

var (
	ErrNotFound  = errors.New("job not found")
	ErrLostLease = errors.New("lease lost: job was reclaimed by another worker")
)

// Store wraps a pgx pool.
type Store struct {
	Pool *pgxpool.Pool
}

// Open connects and verifies the connection.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// Migrate applies the embedded migrations in order, each in a transaction,
// recording them in schema_migrations. Concurrent callers are serialized
// with an advisory lock. upTo limits the applied versions (0 = all); the
// query benchmark uses it to measure before and after the index migration.
func (s *Store) Migrate(ctx context.Context, upTo int) error {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `select pg_advisory_lock(727274)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `select pg_advisory_unlock(727274)`) //nolint:errcheck
	if _, err := conn.Exec(ctx, `create table if not exists schema_migrations (
		version integer primary key, applied_at timestamptz not null default now())`); err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		var version int
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, "migrations/"), "%03d_", &version); err != nil {
			return fmt.Errorf("bad migration name %s", name)
		}
		if upTo > 0 && version > upTo {
			break
		}
		var exists bool
		if err := conn.QueryRow(ctx, `select exists(select 1 from schema_migrations where version = $1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, _ := migrations.ReadFile(name)
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `insert into schema_migrations (version) values ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var result []byte
	err := row.Scan(&j.ID, &j.TenantID, &j.IdempotencyKey, &j.Status, &j.InputSHA256, &j.InputBytes,
		&j.Attempts, &j.MaxAttempts, &j.LastError, &result, &j.Cached, &j.CreatedAt, &j.UpdatedAt,
		&j.StartedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if result != nil {
		j.Result = json.RawMessage(result)
	}
	return j, err
}

// NewJob is what the API knows when a job is submitted.
type NewJob struct {
	TenantID       string
	IdempotencyKey string // optional
	InputSHA256    string
	InputBytes     int64
	MaxAttempts    int
	CachedResult   json.RawMessage // if set, the job is created already succeeded
}

// Create inserts a job. With an idempotency key, a repeated submission
// returns the existing job and created=false.
func (s *Store) Create(ctx context.Context, n NewJob) (job Job, created bool, err error) {
	var key *string
	if n.IdempotencyKey != "" {
		key = &n.IdempotencyKey
	}
	status := Queued
	if n.CachedResult != nil {
		status = Succeeded
	}
	// finished_at comes from the database clock, like created_at: mixing the
	// application's clock in once produced jobs that finished before they
	// were created.
	row := s.Pool.QueryRow(ctx, `
		insert into jobs (tenant_id, idempotency_key, status, input_sha256, input_bytes, max_attempts,
		                  result, cached, finished_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, case when $8 then now() end)
		on conflict (tenant_id, idempotency_key) do nothing
		returning `+jobColumns,
		n.TenantID, key, status, n.InputSHA256, n.InputBytes, n.MaxAttempts,
		nullJSON(n.CachedResult), n.CachedResult != nil)
	job, err = scanJob(row)
	if errors.Is(err, ErrNotFound) && key != nil {
		existing, err := scanJob(s.Pool.QueryRow(ctx,
			`select `+jobColumns+` from jobs where tenant_id = $1 and idempotency_key = $2`, n.TenantID, *key))
		return existing, false, err
	}
	return job, err == nil, err
}

// Get returns a tenant's job.
func (s *Store) Get(ctx context.Context, tenantID, id string) (Job, error) {
	job, err := scanJob(s.Pool.QueryRow(ctx,
		`select `+jobColumns+` from jobs where id = $1 and tenant_id = $2`, id, tenantID))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "22P02" { // malformed uuid
		return Job{}, ErrNotFound
	}
	return job, err
}

// Cursor is a keyset position in a tenant's job list.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// List returns a tenant's jobs newest first, optionally filtered by status,
// starting after the cursor. Keyset pagination keeps every page as cheap as
// the first, unlike OFFSET.
func (s *Store) List(ctx context.Context, tenantID string, status Status, after *Cursor, limit int) ([]Job, error) {
	args := []any{tenantID, limit}
	where := "tenant_id = $1"
	if status != "" {
		args = append(args, status)
		where += fmt.Sprintf(" and status = $%d", len(args))
	}
	if after != nil {
		args = append(args, after.CreatedAt, after.ID)
		where += fmt.Sprintf(" and (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	rows, err := s.Pool.Query(ctx,
		`select `+jobColumns+` from jobs where `+where+` order by created_at desc, id desc limit $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Claim is the result of starting an attempt.
type Claim struct {
	Attempt     int
	MaxAttempts int
	InputSHA256 string
	LeaseToken  string
}

// StartOutcome says what happened when a delivery tried to start an attempt.
type StartOutcome int

const (
	Started     StartOutcome = iota // this delivery now holds the lease
	AlreadyDone                     // job is terminal: acknowledge the delivery
	Busy                            // another live attempt holds the lease: leave the delivery pending
)

// Start begins an attempt: it takes a fresh lease and increments attempts.
// A running job can only be taken over once its lease has expired, so a
// duplicate delivery never steals the lease from a healthy attempt.
func (s *Store) Start(ctx context.Context, id string, lease time.Duration) (Claim, StartOutcome, error) {
	var c Claim
	err := s.Pool.QueryRow(ctx, `
		update jobs
		   set status = 'running', attempts = attempts + 1, lease_token = gen_random_uuid(),
		       lease_expires_at = now() + $2::interval, started_at = coalesce(started_at, now()),
		       updated_at = now()
		 where id = $1
		   and (status in ('queued', 'failed') or (status = 'running' and lease_expires_at < now()))
		returning attempts, max_attempts, input_sha256, lease_token::text`,
		id, lease.String()).Scan(&c.Attempt, &c.MaxAttempts, &c.InputSHA256, &c.LeaseToken)
	if err == nil {
		return c, Started, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, 0, err
	}
	var status Status
	err = s.Pool.QueryRow(ctx, `select status from jobs where id = $1`, id).Scan(&status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Claim{}, AlreadyDone, nil // unknown job: nothing to do
	case err != nil:
		return Claim{}, 0, err
	case status == Running:
		return Claim{}, Busy, nil
	default:
		return Claim{}, AlreadyDone, nil
	}
}

// Heartbeat extends the lease of a running attempt.
func (s *Store) Heartbeat(ctx context.Context, id, lease string, d time.Duration) error {
	tag, err := s.Pool.Exec(ctx, `
		update jobs set lease_expires_at = now() + $3::interval, updated_at = now()
		 where id = $1 and lease_token = $2 and status = 'running'`, id, lease, d.String())
	if err == nil && tag.RowsAffected() == 0 {
		return ErrLostLease
	}
	return err
}

// Succeed records the result if this attempt still holds the lease.
func (s *Store) Succeed(ctx context.Context, id, lease string, result json.RawMessage, cached bool) error {
	tag, err := s.Pool.Exec(ctx, `
		update jobs
		   set status = 'succeeded', result = $3, cached = $4, last_error = null, lease_token = null,
		       lease_expires_at = null, finished_at = now(), updated_at = now()
		 where id = $1 and lease_token = $2`, id, lease, result, cached)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrLostLease
	}
	return err
}

// Fail records a failed attempt. The job becomes 'dead' when the error is
// permanent or attempts are exhausted, otherwise 'failed' (to be retried).
func (s *Store) Fail(ctx context.Context, id, lease, msg string, permanent bool) (Status, error) {
	var status Status
	err := s.Pool.QueryRow(ctx, `
		update jobs
		   set status = case when $4 or attempts >= max_attempts then 'dead'::job_status else 'failed'::job_status end,
		       last_error = $3, lease_token = null, lease_expires_at = null, updated_at = now(),
		       finished_at = case when $4 or attempts >= max_attempts then now() end
		 where id = $1 and lease_token = $2
		returning status`, id, lease, msg, permanent).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrLostLease
	}
	return status, err
}

// Stale returns jobs the queue may have lost: queued or failed jobs not
// touched for `idle`, and running jobs whose lease expired. It touches them
// so concurrent sweepers do not pick the same rows.
func (s *Store) Stale(ctx context.Context, idle time.Duration, limit int) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `
		with stale as (
		    select id from jobs
		     where status in ('queued', 'running', 'failed')
		       and updated_at < now() - $1::interval
		       and (status <> 'running' or lease_expires_at < now())
		     order by updated_at
		     limit $2
		       for update skip locked
		)
		update jobs set updated_at = now() from stale where jobs.id = stale.id
		returning jobs.id::text`, idle.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Ping checks the database for readiness probes.
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func nullJSON(b json.RawMessage) any {
	if b == nil {
		return nil
	}
	return []byte(b)
}
