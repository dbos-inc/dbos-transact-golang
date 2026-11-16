-- Topic a SUSPENDED recv waits on.

ALTER TABLE workflow_status ADD COLUMN "suspend_key" TEXT DEFAULT NULL;
