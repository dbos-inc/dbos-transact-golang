-- Migration 23: Create a partial unique index on (queue_name, deduplication_id)
-- restricted to non-NULL deduplication_id. The new index uses a different name
-- than the original unique constraint (uq_workflow_status_queue_name_dedup_id)
-- to avoid a naming collision; the old constraint is dropped in migration 24.

CREATE UNIQUE INDEX %s IF NOT EXISTS "uq_workflow_status_dedup_id" ON %s."workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL;
