-- Migration 16: Add delay_until_epoch_ms column for delayed queue execution
-- Workflows with this set start in DELAYED status and transition to ENQUEUED after the delay expires.

ALTER TABLE %s.workflow_status ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_delayed" ON %s.workflow_status ("delay_until_epoch_ms") WHERE status = 'DELAYED';