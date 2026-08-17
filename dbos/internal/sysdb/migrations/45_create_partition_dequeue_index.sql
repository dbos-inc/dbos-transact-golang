-- Migration 45: Create the partitioned-queue dequeue index. Extends
-- idx_workflow_status_in_flight with queue_partition_key so lookups scoped to
-- one partition stay selective when many partitions are active.
-- Superseded by idx_workflow_status_partition_dequeue_v2 (migration 46) and
-- dropped by migration 47.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_partition_dequeue" ON %s."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
