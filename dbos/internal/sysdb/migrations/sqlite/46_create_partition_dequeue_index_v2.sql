CREATE INDEX IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2"
    ON "workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid")
    WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
