-- Migration 21: Recreate idx_workflow_status_parent_workflow_id as a partial
-- index. The full index was rarely useful since most workflows have a NULL
-- parent_workflow_id, so this avoids index maintenance on every workflow insert.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_parent_workflow_id" ON %s."workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL;
