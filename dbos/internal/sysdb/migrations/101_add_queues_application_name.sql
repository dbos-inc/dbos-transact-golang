-- Migration 101: Add application_name to queues. NULL means unclaimed.

ALTER TABLE %s."queues" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
