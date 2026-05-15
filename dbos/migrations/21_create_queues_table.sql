-- Migration 21: Create the queues registry table. Persists per-queue
-- configuration (concurrency limits, rate limits, priority, partitioning,
-- polling cadence) so it can be inspected and updated independently of any
-- particular running executor.

CREATE TABLE %s.queues (
    queue_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec DOUBLE PRECISION,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);
