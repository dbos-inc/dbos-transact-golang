-- Migration 19: Recreate idx_workflow_status_forked_from as a partial index.
-- The full index was rarely useful since most workflows have a NULL forked_from
-- value, so this avoids index maintenance on every workflow insert.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_forked_from" ON %s."workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL;
