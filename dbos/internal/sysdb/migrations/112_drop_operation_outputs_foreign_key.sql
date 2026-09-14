-- Migration 112: steps no longer cascade from workflow_status.

ALTER TABLE %s."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_foreign";

ALTER TABLE %s."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_fkey";
