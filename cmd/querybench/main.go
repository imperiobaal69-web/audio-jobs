// Command querybench measures the hot queries on a seeded jobs table before
// and after the index migration and writes docs/results/query-optimization.md.
//
// It creates a scratch database, applies migration 001 only, seeds N rows
// with a skewed tenant distribution and a realistic status mix, runs each
// query several times under EXPLAIN (ANALYZE, BUFFERS), then applies the
// remaining migrations and repeats. The scratch database is dropped at the end.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imperiobaal69-web/audio-jobs/internal/store"
)

type query struct {
	name string
	what string
	sql  string
}

type measurement struct {
	medianMs float64
	plan     string // root-to-leaf node types
	buffers  int64  // shared hit + read of the median run
	rows     int64
}

func main() {
	admin := flag.String("admin-url", "postgres://postgres@127.0.0.1:5440/postgres?sslmode=disable", "connection to create the scratch database")
	rows := flag.Int("rows", 1_000_000, "jobs to seed")
	runs := flag.Int("runs", 7, "executions per query (median reported)")
	out := flag.String("out", "docs/results/query-optimization.md", "report path")
	keep := flag.Bool("keep", false, "keep the scratch database")
	flag.Parse()
	ctx := context.Background()

	adminConn, err := pgx.Connect(ctx, *admin)
	must(err)
	defer adminConn.Close(ctx)
	db := "audiojobs_querybench"
	_, _ = adminConn.Exec(ctx, "drop database if exists "+db+" with (force)")
	_, err = adminConn.Exec(ctx, "create database "+db)
	must(err)
	if !*keep {
		defer adminConn.Exec(context.Background(), "drop database if exists "+db+" with (force)") //nolint:errcheck
	}
	st, err := store.Open(ctx, strings.Replace(*admin, "/postgres?", "/"+db+"?", 1))
	must(err)
	defer st.Close()

	must(st.Migrate(ctx, 1))
	log.Printf("seeding %d rows...", *rows)
	t0 := time.Now()
	_, err = st.Pool.Exec(ctx, seedSQL, *rows)
	must(err)
	_, err = st.Pool.Exec(ctx, "vacuum analyze jobs")
	must(err)
	log.Printf("seeded in %s", time.Since(t0).Round(time.Second))

	var tableSize string
	var heavyTenant string
	var heavyRows, unfinished int64
	must(st.Pool.QueryRow(ctx, `select pg_size_pretty(pg_total_relation_size('jobs'))`).Scan(&tableSize))
	must(st.Pool.QueryRow(ctx, `select tenant_id, count(*) from jobs group by 1 order by 2 desc limit 1`).Scan(&heavyTenant, &heavyRows))
	must(st.Pool.QueryRow(ctx, `select count(*) from jobs where status in ('queued','running','failed')`).Scan(&unfinished))

	// A keyset cursor at the same depth as OFFSET 5000, so both return the same page.
	var cursorAt time.Time
	var cursorID string
	must(st.Pool.QueryRow(ctx, `select created_at, id from jobs where tenant_id = $1
		order by created_at desc, id desc offset 4999 limit 1`, heavyTenant).Scan(&cursorAt, &cursorID))

	cols := "id, tenant_id, status, input_sha256, attempts, created_at, finished_at"
	queries := []query{
		{"list-first-page", "Tenant's newest 50 jobs (dashboard first page)",
			fmt.Sprintf(`select %s from jobs where tenant_id = '%s' order by created_at desc, id desc limit 50`, cols, heavyTenant)},
		{"list-offset-5000", "Page 101 with OFFSET 5000 (what a naive paginator does)",
			fmt.Sprintf(`select %s from jobs where tenant_id = '%s' order by created_at desc, id desc offset 5000 limit 50`, cols, heavyTenant)},
		{"list-keyset-5000", "The same page with a keyset cursor (what the API does)",
			fmt.Sprintf(`select %s from jobs where tenant_id = '%s' and (created_at, id) < ('%s', '%s')
			order by created_at desc, id desc limit 50`, cols, heavyTenant, cursorAt.UTC().Format(time.RFC3339Nano), cursorID)},
		{"list-dead", "Tenant's failed jobs (status filter, rare status)",
			fmt.Sprintf(`select %s from jobs where tenant_id = '%s' and status = 'dead' order by created_at desc, id desc limit 50`, cols, heavyTenant)},
		{"sweeper", "Sweeper: unfinished jobs idle for 2 minutes",
			`select id from jobs where status in ('queued', 'running', 'failed') and updated_at < now() - interval '2 minutes'
			and (status <> 'running' or lease_expires_at < now()) order by updated_at limit 500`},
	}

	before := measureAll(ctx, st, queries, *runs)
	writesBefore := measureWrites(ctx, st, *runs)
	must(st.Migrate(ctx, 0))
	_, err = st.Pool.Exec(ctx, "analyze jobs")
	must(err)
	var indexes []string
	r, err := st.Pool.Query(ctx, `select indexname || ': ' || pg_size_pretty(pg_relation_size(indexname::regclass))
		from pg_indexes where tablename = 'jobs' order by indexname`)
	must(err)
	for r.Next() {
		var s string
		must(r.Scan(&s))
		indexes = append(indexes, s)
	}
	after := measureAll(ctx, st, queries, *runs)
	writesAfter := measureWrites(ctx, st, *runs)

	var pgVersion string
	must(st.Pool.QueryRow(ctx, `show server_version`).Scan(&pgVersion))
	report := render(*rows, tableSize, heavyTenant, heavyRows, unfinished, pgVersion, indexes, queries, before, after)
	report += renderWrites(writesBefore, writesAfter)
	must(os.WriteFile(*out, []byte(report), 0o644))
	fmt.Print(report)
}

func measureAll(ctx context.Context, st *store.Store, qs []query, runs int) map[string]measurement {
	out := map[string]measurement{}
	for _, q := range qs {
		type run struct {
			ms      float64
			plan    string
			buffers int64
			rows    int64
		}
		var rs []run
		for i := 0; i < runs; i++ {
			var raw []byte
			must(st.Pool.QueryRow(ctx, "explain (analyze, buffers, format json) "+q.sql).Scan(&raw))
			var doc []struct {
				Plan          map[string]any `json:"Plan"`
				ExecutionTime float64        `json:"Execution Time"`
			}
			must(json.Unmarshal(raw, &doc))
			p := doc[0].Plan
			rs = append(rs, run{
				ms:      doc[0].ExecutionTime,
				plan:    planShape(p),
				buffers: int64(num(p["Shared Hit Blocks"]) + num(p["Shared Read Blocks"])),
				rows:    int64(num(p["Actual Rows"])),
			})
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].ms < rs[j].ms })
		m := rs[len(rs)/2]
		out[q.name] = measurement{medianMs: m.ms, plan: m.plan, buffers: m.buffers, rows: m.rows}
		log.Printf("%-18s %9.3f ms  %s", q.name, m.ms, m.plan)
	}
	return out
}

// lifecycle phases of 20,000 new jobs, as set-based statements: the work the
// API and workers do per job, in bulk.
var lifecycle = []struct{ name, sql string }{
	{"insert (submit)", `insert into jobs (tenant_id, input_sha256, input_bytes, idempotency_key)
		select 'tenant-' || lpad((g % 200)::text, 3, '0'), md5(g::text), 1000, 'wb-' || g
		from generate_series(1, 20000) g`},
	{"update to running (start attempt)", `update jobs set status = 'running', attempts = attempts + 1,
		lease_token = gen_random_uuid(), lease_expires_at = now() + interval '30 seconds', started_at = now(), updated_at = now()
		where idempotency_key like 'wb-%'`},
	{"update to succeeded (record result)", `update jobs set status = 'succeeded', result = '{"version":"v1"}',
		lease_token = null, lease_expires_at = null, finished_at = now(), updated_at = now()
		where idempotency_key like 'wb-%'`},
}

// measureWrites times the lifecycle (median of runs), deleting the rows
// after each run.
func measureWrites(ctx context.Context, st *store.Store, runs int) []float64 {
	samples := make([][]float64, len(lifecycle))
	for i := 0; i < runs; i++ {
		for p, phase := range lifecycle {
			t := time.Now()
			_, err := st.Pool.Exec(ctx, phase.sql)
			must(err)
			samples[p] = append(samples[p], float64(time.Since(t).Microseconds())/1000)
		}
		_, err := st.Pool.Exec(ctx, `delete from jobs where idempotency_key like 'wb-%'`)
		must(err)
		_, err = st.Pool.Exec(ctx, `vacuum jobs`)
		must(err)
	}
	out := make([]float64, len(lifecycle))
	for p := range samples {
		sort.Float64s(samples[p])
		out[p] = samples[p][len(samples[p])/2]
	}
	return out
}

func renderWrites(before, after []float64) string {
	var b strings.Builder
	b.WriteString("\n## Write cost of the indexes\n\nThe lifecycle of 20,000 new jobs as set-based statements (median of the runs):\n\n")
	b.WriteString("| phase | before (ms) | after (ms) | change |\n|---|---|---|---|\n")
	var tb, ta float64
	for i, phase := range lifecycle {
		fmt.Fprintf(&b, "| %s | %.1f | %.1f | %+.0f%% |\n", phase.name, before[i], after[i], (after[i]/before[i]-1)*100)
		tb += before[i]
		ta += after[i]
	}
	fmt.Fprintf(&b, "| total | %.1f | %.1f | %+.0f%% |\n", tb, ta, (ta/tb-1)*100)
	return b.String()
}

// planShape lists node types from the root down the first child chain,
// e.g. "Limit > Sort > Seq Scan".
func planShape(p map[string]any) string {
	var parts []string
	for p != nil {
		name, _ := p["Node Type"].(string)
		if idx, ok := p["Index Name"].(string); ok {
			name += " (" + idx + ")"
		}
		parts = append(parts, name)
		children, _ := p["Plans"].([]any)
		if len(children) == 0 {
			break
		}
		p, _ = children[0].(map[string]any)
	}
	return strings.Join(parts, " > ")
}

func render(rows int, size, tenant string, tenantRows, unfinished int64, pg string, indexes []string,
	qs []query, before, after map[string]measurement) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Query optimization results\n\nGenerated by `go run ./cmd/querybench`; do not edit by hand.\n")
	fmt.Fprintf(&b, "Run %s UTC, PostgreSQL %s on one machine.\n\n", time.Now().UTC().Format("2006-01-02 15:04"), pg)
	fmt.Fprintf(&b, "Data: %d jobs (%s with indexes), 200 tenants with a skewed distribution; the heaviest tenant (`%s`) has %d jobs. %d jobs are unfinished (queued / running / failed).\n\n",
		rows, size, tenant, tenantRows, unfinished)
	fmt.Fprintf(&b, "Before: primary key and the idempotency unique constraint only (migration 001). After: migration 002.\n\n")
	b.WriteString("| query | before (ms) | after (ms) | speed-up | buffers before → after | plan after |\n|---|---|---|---|---|---|\n")
	for _, q := range qs {
		bm, am := before[q.name], after[q.name]
		fmt.Fprintf(&b, "| %s | %.2f | %.3f | %.0fx | %d → %d | %s |\n", q.what, bm.medianMs, am.medianMs,
			bm.medianMs/max(am.medianMs, 0.001), bm.buffers, am.buffers, am.plan)
	}
	b.WriteString("\nPlans before:\n\n")
	for _, q := range qs {
		fmt.Fprintf(&b, "- %s: %s\n", q.name, before[q.name].plan)
	}
	b.WriteString("\nIndexes after:\n\n")
	for _, ix := range indexes {
		fmt.Fprintf(&b, "- %s\n", ix)
	}
	b.WriteString("\nTimes are the median of the runs' `Execution Time`; buffers are shared blocks hit + read at the plan root.\n")
	return b.String()
}

// 200 tenants, skewed (tenant k gets a share proportional to 1/(k+1));
// 98.5% succeeded, 0.5% dead, and a small unfinished tail with recent
// timestamps, which is what a table with history looks like.
const seedSQL = `
insert into jobs (tenant_id, status, input_sha256, input_bytes, attempts, max_attempts, result, cached,
                  created_at, updated_at, started_at, finished_at, lease_expires_at, last_error)
select
    'tenant-' || lpad((floor(200 * power(random(), 3)))::int::text, 3, '0'),
    st.status,
    md5(g::text) || md5((g * 7)::text),
    1920044,
    case when st.status in ('queued') then 0 else 1 end,
    5,
    case when st.status = 'succeeded' then
        jsonb_build_object('version', 'v1', 'duration_ms', 60000, 'peak_dbfs', -3.1, 'rms_dbfs', -19.4,
                           'speech_ratio', 0.79, 'silences', '[]'::jsonb)
    end,
    random() < 0.1,
    ts.created,
    case when st.status in ('queued', 'running', 'failed') then now() - random() * interval '10 minutes'
         else ts.created + interval '3 seconds' end,
    case when st.status <> 'queued' then ts.created + interval '1 second' end,
    case when st.status in ('succeeded', 'dead') then ts.created + interval '3 seconds' end,
    case when st.status = 'running' then now() + (random() - 0.5) * interval '1 minute' end,
    case when st.status in ('dead', 'failed') then 'analyze: unexpected EOF' end
from generate_series(1, $1) as g
cross join lateral (select random() + 0 * g as r) rnd -- correlated with g, or Postgres evaluates it once
cross join lateral (select case
        when rnd.r < 0.985 then 'succeeded'
        when rnd.r < 0.990 then 'dead'
        when rnd.r < 0.995 then 'queued'
        when rnd.r < 0.998 then 'running'
        else 'failed' end::job_status as status) st
cross join lateral (select case
        when st.status in ('queued', 'running', 'failed') then now() - random() * interval '15 minutes'
        else now() - random() * interval '180 days' end as created) ts`

func num(v any) float64 {
	f, _ := v.(float64)
	return f
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
