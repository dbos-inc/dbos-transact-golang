CREATE INDEX IF NOT EXISTS "idx_workflow_status_failed"
    ON "workflow_status" ("status", "created_at")
    WHERE "status" IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED');
