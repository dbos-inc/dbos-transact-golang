-- Migration 109: workflow inputs and outputs move to their own tables.

CREATE TABLE IF NOT EXISTS %s."workflow_input" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE TABLE IF NOT EXISTS %s."workflow_output" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE INDEX IF NOT EXISTS "idx_workflow_input_retention"
    ON %s."workflow_input" ("retention_timestamp");

CREATE INDEX IF NOT EXISTS "idx_workflow_output_retention"
    ON %s."workflow_output" ("retention_timestamp");
