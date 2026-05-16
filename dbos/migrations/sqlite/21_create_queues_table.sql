CREATE TABLE queues (
    queue_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec REAL,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec REAL NOT NULL DEFAULT 1.0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
