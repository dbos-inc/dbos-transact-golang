CREATE INDEX IF NOT EXISTS idx_operation_outputs_retention
    ON operation_outputs (retention_timestamp);
