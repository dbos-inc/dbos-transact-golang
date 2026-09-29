-- Migration 122: Partial index for the workflow timeouts monitor.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_deadline" ON %s."workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
