-- idx_workflow_status_deadline with a predicate that also covers SUSPENDED workflows.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_deadline_v2" ON "workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED', 'SUSPENDED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
