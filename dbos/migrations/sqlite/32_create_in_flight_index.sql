CREATE INDEX IF NOT EXISTS "idx_workflow_status_in_flight"
    ON "workflow_status" ("queue_name", "status", "priority", "created_at")
    WHERE "status" IN ('ENQUEUED', 'PENDING');
