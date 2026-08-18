-- Migration 102: Add application_name to workflow_schedules. NULL means
-- unclaimed.

ALTER TABLE %s."workflow_schedules" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
