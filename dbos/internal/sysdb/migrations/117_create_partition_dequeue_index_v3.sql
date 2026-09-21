-- Migration 117: Recreate the partitioned-queue dequeue index with
-- application_name INCLUDEd, as idx_workflow_status_in_flight_v2 does.
-- Supersedes idx_workflow_status_partition_dequeue_v2 (migration 46), dropped
-- by migration 118.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_partition_dequeue_v3" ON %s."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") INCLUDE ("application_name") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
