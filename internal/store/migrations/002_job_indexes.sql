-- Added after measuring the two hot queries on 1M rows (see
-- docs/query-optimization.md).

-- Tenant job listing, newest first, keyset-paginated on (created_at, id).
create index jobs_tenant_created_id on jobs (tenant_id, created_at desc, id desc);

-- The sweeper only looks at non-terminal jobs, which are a tiny fraction of
-- the table once it has history; a partial index keeps it independent of
-- table size.
create index jobs_unfinished_updated on jobs (updated_at)
    where status in ('queued', 'running', 'failed');
