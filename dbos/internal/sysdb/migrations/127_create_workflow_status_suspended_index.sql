-- Migration 127: Partial index for the sweep that re-enqueues SUSPENDED workflows.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_suspended" ON %s."workflow_status" ("delay_until_epoch_ms") WHERE "status" = 'SUSPENDED';
