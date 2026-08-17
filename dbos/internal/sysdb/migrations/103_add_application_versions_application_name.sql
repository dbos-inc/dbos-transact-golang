-- Migration 103: Add application_name to application_versions. NULL means
-- unclaimed.

ALTER TABLE %s."application_versions" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
