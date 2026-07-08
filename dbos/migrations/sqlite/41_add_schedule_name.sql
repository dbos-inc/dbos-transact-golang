ALTER TABLE workflow_status ADD COLUMN "schedule_name" TEXT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_schedule_name" ON "workflow_status" ("schedule_name") WHERE "schedule_name" IS NOT NULL;
