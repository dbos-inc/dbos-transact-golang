-- Migration 111: index the step sweep order, online.

CREATE INDEX %s IF NOT EXISTS "idx_operation_outputs_retention"
    ON %s."operation_outputs" ("retention_timestamp");
