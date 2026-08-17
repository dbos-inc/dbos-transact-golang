-- Migration 46: Recreate the partitioned-queue dequeue index with a trailing
-- workflow_uuid, which totalizes the dequeue order so per-partition head
-- probes are pure top-1 index seeks. Supersedes
-- idx_workflow_status_partition_dequeue (migration 45), dropped by migration 47.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2" ON %s."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
