-- Migration 12: Add consumed column to notifications and index for unconsumed lookups.

ALTER TABLE %s.notifications ADD COLUMN consumed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications_unconsumed" ON %s.notifications (destination_uuid, topic) WHERE consumed = FALSE;
