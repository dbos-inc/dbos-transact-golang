-- Partial index for the sweep that re-enqueues SUSPENDED workflows.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_suspended" ON "workflow_status" ("delay_until_epoch_ms") WHERE "status" = 'SUSPENDED';
