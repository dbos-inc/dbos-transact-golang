-- Migration 15: Add missing columns to workflow_schedules table
-- Matches TypeScript and Python SDK schema

ALTER TABLE %s.workflow_schedules ADD COLUMN IF NOT EXISTS last_fired_at TEXT;
ALTER TABLE %s.workflow_schedules ADD COLUMN IF NOT EXISTS automatic_backfill BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE %s.workflow_schedules ADD COLUMN IF NOT EXISTS cron_timezone TEXT;
ALTER TABLE %s.workflow_schedules ADD COLUMN IF NOT EXISTS queue_name TEXT;