ALTER TABLE "workflow_status" ADD COLUMN "completed_at" BIGINT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_completed_at" ON "workflow_status" ("completed_at") WHERE "completed_at" IS NOT NULL;
