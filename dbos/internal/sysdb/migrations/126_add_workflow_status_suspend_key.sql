-- Migration 126: Topic a SUSPENDED recv waits on.

ALTER TABLE %s."workflow_status"
    ADD COLUMN IF NOT EXISTS "suspend_key" TEXT DEFAULT NULL;
