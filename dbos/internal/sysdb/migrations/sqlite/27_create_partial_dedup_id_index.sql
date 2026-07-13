CREATE UNIQUE INDEX IF NOT EXISTS "uq_workflow_status_dedup_id"
    ON "workflow_status" ("queue_name", "deduplication_id")
    WHERE "deduplication_id" IS NOT NULL;
