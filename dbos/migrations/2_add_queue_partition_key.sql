-- Migration 2: Add queue_partition_key column to workflow_status table
-- This enables partitioned queues where workflows can be distributed across
-- dynamically created queue partitions with separate concurrency limits per partition.

ALTER TABLE %s.workflow_status
ADD COLUMN queue_partition_key TEXT;

