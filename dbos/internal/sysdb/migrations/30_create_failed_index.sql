-- Migration 26: Create a partial index on workflow_status restricted to
-- failed workflows (ERROR, CANCELLED, MAX_RECOVERY_ATTEMPTS_EXCEEDED) for
-- rapid troubleshooting queries.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_failed" ON %s."workflow_status" ("status", "created_at") WHERE "status" IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED');
