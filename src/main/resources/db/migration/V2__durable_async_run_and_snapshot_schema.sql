create table async_runs (
    id text primary key,
    operation_type text not null,
    status text not null check (
        status in (
            'queued',
            'running',
            'waiting_retry',
            'completed',
            'completed_with_warnings',
            'failed',
            'cancelled',
            'superseded'
        )
    ),
    scope_type text not null,
    scope_id text not null,
    user_id text not null,
    parent_run_id text references async_runs (id),
    current_step text,
    completed_steps integer not null default 0 check (completed_steps >= 0),
    total_steps integer not null check (total_steps >= 0),
    completed_units integer check (completed_units is null or completed_units >= 0),
    total_units integer check (total_units is null or total_units >= 0),
    warnings_json jsonb not null default '[]'::jsonb check (jsonb_typeof(warnings_json) = 'array'),
    metadata_json jsonb not null default '{}'::jsonb check (jsonb_typeof(metadata_json) = 'object'),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (completed_steps <= total_steps),
    check (completed_units is null or total_units is null or completed_units <= total_units)
);

create index idx_async_runs_user_status on async_runs (user_id, status);
create index idx_async_runs_scope on async_runs (scope_type, scope_id);

create table search_requests (
    id text primary key,
    user_id text not null,
    workflow text not null,
    keywords text,
    question text,
    retrieval_filters_json jsonb not null default '{}'::jsonb check (jsonb_typeof(retrieval_filters_json) = 'object'),
    initial_sort_json jsonb not null default '{}'::jsonb check (jsonb_typeof(initial_sort_json) = 'object'),
    created_at timestamptz not null default now()
);

create index idx_search_requests_user_created on search_requests (user_id, created_at desc);

create table search_runs (
    id text primary key,
    async_run_id text not null unique references async_runs (id) on delete restrict,
    search_request_id text not null references search_requests (id) on delete restrict,
    workflow text not null,
    workflow_config_version integer not null check (workflow_config_version > 0),
    result_snapshot_id text not null unique,
    created_at timestamptz not null default now()
);

create index idx_search_runs_request on search_runs (search_request_id);

create table result_snapshots (
    id text primary key,
    search_run_id text not null unique references search_runs (id) on delete restrict,
    user_id text not null,
    status text not null,
    schema_json jsonb not null check (jsonb_typeof(schema_json) = 'object'),
    default_sort_json jsonb not null check (jsonb_typeof(default_sort_json) = 'array'),
    created_at timestamptz not null default now(),
    ready_at timestamptz
);

alter table search_runs
    add constraint fk_search_runs_result_snapshot
    foreign key (result_snapshot_id) references result_snapshots (id)
    deferrable initially deferred;

create index idx_result_snapshots_user_status on result_snapshots (user_id, status);

create table async_run_steps (
    id text primary key,
    async_run_id text not null references async_runs (id) on delete cascade,
    workflow_step_id text not null,
    status text not null check (
        status in (
            'queued',
            'running',
            'waiting_retry',
            'completed',
            'completed_with_warnings',
            'failed',
            'cancelled',
            'superseded'
        )
    ),
    required boolean not null,
    attempt_count integer not null default 0 check (attempt_count >= 0),
    max_attempts integer not null check (max_attempts > 0),
    last_error text,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    unique (async_run_id, workflow_step_id),
    check (attempt_count <= max_attempts)
);

create index idx_async_run_steps_run_status on async_run_steps (async_run_id, status);

create table worker_contributions (
    id text primary key,
    search_run_id text not null references search_runs (id) on delete cascade,
    workflow_step_id text not null,
    chunk_id text not null,
    contribution_type text not null,
    rank integer check (rank is null or rank > 0),
    score numeric(6, 4),
    payload_json jsonb not null default '{}'::jsonb check (jsonb_typeof(payload_json) = 'object'),
    created_at timestamptz not null default now(),
    unique (search_run_id, workflow_step_id, chunk_id)
);

create index idx_worker_contributions_run_step on worker_contributions (search_run_id, workflow_step_id);
create index idx_worker_contributions_run_chunk on worker_contributions (search_run_id, chunk_id);

create table result_items (
    id text primary key,
    result_snapshot_id text not null references result_snapshots (id) on delete cascade,
    chunk_id text not null,
    parent_entity_id text not null,
    parent_title text not null,
    parent_type text not null,
    source_name text not null,
    ticker text not null,
    company_name text not null,
    sector text not null,
    region text not null,
    published_at timestamptz not null,
    author text,
    chunk_index integer not null check (chunk_index >= 0),
    chunk_text text not null,
    relevance_score numeric(4, 2) check (relevance_score is null or (relevance_score >= 0 and relevance_score <= 10)),
    lexical_rank integer check (lexical_rank is null or lexical_rank > 0),
    default_rank integer not null check (default_rank > 0),
    source_contributions_json jsonb not null default '[]'::jsonb check (jsonb_typeof(source_contributions_json) = 'array'),
    created_at timestamptz not null default now(),
    unique (result_snapshot_id, chunk_id)
);

create index idx_result_items_snapshot_default_rank on result_items (result_snapshot_id, default_rank);
create index idx_result_items_snapshot_ticker on result_items (result_snapshot_id, ticker);
create index idx_result_items_snapshot_sector on result_items (result_snapshot_id, sector);
create index idx_result_items_snapshot_region on result_items (result_snapshot_id, region);
create index idx_result_items_snapshot_relevance on result_items (result_snapshot_id, relevance_score desc nulls last);
