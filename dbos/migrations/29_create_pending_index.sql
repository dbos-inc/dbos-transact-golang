-- Migration 25: Create a partial index on workflow_status restricted to
-- PENDING workflows. Used by the recovery query. Splitting the old
-- workflow_status_status_index into two partial indexes (this and
-- idx_workflow_status_failed) reduces the number of times the index is
-- updated during a typical workflow lifetime
-- (ENQUEUED -> PENDING -> SUCCESS only updates this index on entry/exit
-- of PENDING).

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_pending" ON %s."workflow_status" ("created_at") WHERE "status" = 'PENDING';
