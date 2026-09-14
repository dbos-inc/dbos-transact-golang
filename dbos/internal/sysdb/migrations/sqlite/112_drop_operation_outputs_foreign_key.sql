-- Rebuild without the workflow_status foreign key; SQLite cannot drop a constraint in place.

CREATE TABLE operation_outputs_new (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    started_at_epoch_ms INTEGER,
    completed_at_epoch_ms INTEGER,
    serialization TEXT,
    application_name TEXT DEFAULT NULL,
    retention_timestamp INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
    PRIMARY KEY (workflow_uuid, function_id)
);
INSERT INTO operation_outputs_new (workflow_uuid, function_id, function_name, output,
    error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization,
    application_name, retention_timestamp)
SELECT workflow_uuid, function_id, function_name, output, error, child_workflow_id,
    started_at_epoch_ms, completed_at_epoch_ms, serialization, application_name,
    COALESCE(retention_timestamp, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
FROM operation_outputs;
DROP TABLE operation_outputs;
ALTER TABLE operation_outputs_new RENAME TO operation_outputs;
CREATE INDEX IF NOT EXISTS idx_operation_outputs_retention
    ON operation_outputs (retention_timestamp);
CREATE INDEX IF NOT EXISTS idx_operation_outputs_completed_at_function_name
    ON operation_outputs (completed_at_epoch_ms, function_name);
