-- Jobs are the source of truth. Redis only carries the queue; if Redis loses
-- its data, the sweeper rebuilds the queue from this table.
create type job_status as enum ('queued', 'running', 'succeeded', 'failed', 'dead');

create table jobs (
    id               uuid primary key default gen_random_uuid(),
    tenant_id        text        not null,
    idempotency_key  text,
    status           job_status  not null default 'queued',
    input_sha256     text        not null,
    input_bytes      bigint      not null,
    attempts         integer     not null default 0,
    max_attempts     integer     not null default 5,
    lease_token      uuid,
    lease_expires_at timestamptz,
    last_error       text,
    result           jsonb,
    cached           boolean     not null default false,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    started_at       timestamptz,
    finished_at      timestamptz,
    constraint jobs_tenant_idempotency_key unique (tenant_id, idempotency_key),
    constraint jobs_attempts_nonnegative check (attempts >= 0)
);
