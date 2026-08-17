-- Migration 104: Add application_name to operation_outputs, denormalized from
-- the parent workflow so step observability filters without a join.

ALTER TABLE %s."operation_outputs" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
