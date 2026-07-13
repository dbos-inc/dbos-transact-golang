CREATE INDEX IF NOT EXISTS "idx_workflow_status_parent_workflow_id"
    ON "workflow_status" ("parent_workflow_id")
    WHERE "parent_workflow_id" IS NOT NULL;
