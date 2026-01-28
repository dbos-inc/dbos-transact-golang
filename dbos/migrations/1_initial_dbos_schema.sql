CREATE TABLE %s.workflow_status (
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
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    application_version TEXT,
    application_id TEXT,
    class_name VARCHAR(255) DEFAULT NULL,
    config_name VARCHAR(255) DEFAULT NULL,
    recovery_attempts BIGINT DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    inputs TEXT,
    started_at_epoch_ms BIGINT,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON %s.workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON %s.workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON %s.workflow_status (status);

ALTER TABLE %s.workflow_status
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id
UNIQUE (queue_name, deduplication_id);

CREATE TABLE %s.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES %s.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE %s.notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(), -- Built-in function
    FOREIGN KEY (destination_uuid) REFERENCES %s.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON %s.notifications (destination_uuid, topic);

CREATE TABLE %s.workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES %s.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE %s.streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES %s.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE %s.event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC(38,0),
    update_time NUMERIC(38,15),
    PRIMARY KEY (service_name, workflow_fn_name, key)
);