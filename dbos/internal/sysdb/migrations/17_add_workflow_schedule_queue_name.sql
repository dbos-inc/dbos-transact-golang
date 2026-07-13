-- Migration 17: Add queue_name column to workflow_schedules
-- Lets schedules target a user-defined queue; NULL falls back to the internal queue.

ALTER TABLE %s.workflow_schedules ADD COLUMN "queue_name" TEXT DEFAULT NULL;
