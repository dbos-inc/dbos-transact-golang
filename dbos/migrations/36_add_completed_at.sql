-- Migration 36: Add completed_at column to workflow_status, recording when a
-- workflow reached a terminal state. ADD COLUMN with no default is catalog-only;
-- the partial index built in the same transaction covers zero rows, so no
-- CONCURRENTLY is needed.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "completed_at" BIGINT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_completed_at" ON %s."workflow_status" ("completed_at") WHERE "completed_at" IS NOT NULL;
