-- Ported from python/dbos/_migration.py:sqlite_migration_one, with the
-- event_dispatch_kv table added to match Go's pg migration 1.
--
-- Uses these SQLite-flavor conventions:
--   * INTEGER for epoch-ms columns (BIGINT is an alias for INTEGER in SQLite).
--   * Timestamp / UUID columns have no DEFAULT — Go callers supply
--     time.Now().UnixMilli() and uuid.NewString() explicitly so the schema
--     does not depend on driver-side UDFs.
--   * Foreign keys require PRAGMA foreign_keys = ON, set on connect.

CREATE TABLE workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    application_version TEXT,
    application_id TEXT,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    recovery_attempts INTEGER DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms INTEGER,
    workflow_deadline_epoch_ms INTEGER,
    inputs TEXT,
    started_at_epoch_ms INTEGER,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON workflow_status (status);

CREATE UNIQUE INDEX uq_workflow_status_queue_name_dedup_id
    ON workflow_status (queue_name, deduplication_id);

CREATE TABLE operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE notifications (
    message_uuid TEXT NOT NULL PRIMARY KEY,
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms INTEGER NOT NULL,
    FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);

CREATE TABLE workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC,
    update_time NUMERIC,
    PRIMARY KEY (service_name, workflow_fn_name, key)
);
