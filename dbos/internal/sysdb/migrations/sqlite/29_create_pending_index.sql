CREATE INDEX IF NOT EXISTS "idx_workflow_status_pending"
    ON "workflow_status" ("created_at")
    WHERE "status" = 'PENDING';
