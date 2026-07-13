-- Migration 5: Add started_at_epoch_ms and completed_at_epoch_ms columns to operation_outputs table
-- This enables visualization of step duration

ALTER TABLE %s.operation_outputs
ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;

