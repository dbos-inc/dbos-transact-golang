-- Migration 100: Add application_name to workflow_status, the first migration
-- of the cross-SDK shared history (see SharedMigrationBase). NULL means
-- unclaimed: any application may read and claim the row. One table per
-- migration, so a blocked table does not hold the others' locks.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
