CREATE TABLE IF NOT EXISTS workflow_input (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
);

CREATE TABLE IF NOT EXISTS workflow_output (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
);

CREATE INDEX IF NOT EXISTS idx_workflow_input_retention
    ON workflow_input (retention_timestamp);
CREATE INDEX IF NOT EXISTS idx_workflow_output_retention
    ON workflow_output (retention_timestamp);
