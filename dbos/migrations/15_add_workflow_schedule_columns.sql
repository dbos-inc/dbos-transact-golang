-- Migration 15: Add columns to workflow_schedules for enhanced scheduling
-- Adds last_fired_at, automatic_backfill, and cron_timezone columns.

ALTER TABLE %s.workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
ALTER TABLE %s.workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE %s.workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;