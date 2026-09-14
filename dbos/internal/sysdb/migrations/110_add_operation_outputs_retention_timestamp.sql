-- Migration 110: sweep order for the step payload sweep.

ALTER TABLE %s."operation_outputs"
    ADD COLUMN IF NOT EXISTS "retention_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint;
