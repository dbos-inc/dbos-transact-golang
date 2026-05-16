CREATE INDEX IF NOT EXISTS "idx_workflow_status_rate_limited"
    ON "workflow_status" ("queue_name", "started_at_epoch_ms")
    WHERE "rate_limited" = TRUE;
