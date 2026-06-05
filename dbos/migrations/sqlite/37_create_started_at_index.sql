CREATE INDEX IF NOT EXISTS "idx_workflow_status_started_at" ON "workflow_status" ("started_at_epoch_ms") WHERE "started_at_epoch_ms" IS NOT NULL;
