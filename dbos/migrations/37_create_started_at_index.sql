-- Migration 37: Create a partial index on started_at_epoch_ms to speed up
-- queries filtering workflows by dequeue time.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_started_at" ON %s."workflow_status" ("started_at_epoch_ms") WHERE "started_at_epoch_ms" IS NOT NULL;
