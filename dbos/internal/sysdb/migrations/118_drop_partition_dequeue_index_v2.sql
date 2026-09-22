-- Migration 118: Drop the v2 partitioned-queue dequeue index (migration 46),
-- superseded by idx_workflow_status_partition_dequeue_v3 (migration 117).

DROP INDEX %s IF EXISTS %s."idx_workflow_status_partition_dequeue_v2";
