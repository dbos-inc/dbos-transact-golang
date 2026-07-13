CREATE INDEX IF NOT EXISTS "idx_workflow_status_forked_from"
    ON "workflow_status" ("forked_from")
    WHERE "forked_from" IS NOT NULL;
