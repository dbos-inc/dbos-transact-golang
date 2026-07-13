-- Migration 41: Add a schedule_name column to workflow_status recording which
-- named schedule (if any) enqueued the workflow. ADD COLUMN with no default is
-- catalog-only; the partial index built in the same transaction covers zero
-- rows (no existing row has a non-NULL schedule_name), so no CONCURRENTLY is
-- needed. The index supports filtering workflows by schedule name.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "schedule_name" TEXT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_schedule_name" ON %s."workflow_status" ("schedule_name") WHERE "schedule_name" IS NOT NULL;
