ALTER TABLE workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
ALTER TABLE workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;
