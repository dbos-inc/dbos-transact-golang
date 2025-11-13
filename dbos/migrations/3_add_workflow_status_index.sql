-- Migration 3: Add index on workflow_status for queue, status, and started_at_epoch_ms
-- This index improves query performance for queue operations that filter by queue_name, status, and started_at_epoch_ms

CREATE INDEX "idx_workflow_status_queue_status_started" ON %s."workflow_status" ("queue_name", "status", "started_at_epoch_ms");

