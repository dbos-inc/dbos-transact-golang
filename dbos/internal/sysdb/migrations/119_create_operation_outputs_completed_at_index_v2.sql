-- Migration 119: Recreate the step-completion index with application_name
-- INCLUDEd, as idx_workflow_status_in_flight_v2 does. Supersedes
-- idx_operation_outputs_completed_at_function_name (migration 19), dropped by
-- migration 120.

CREATE INDEX %s IF NOT EXISTS "idx_operation_outputs_completed_at_function_name_v2" ON %s."operation_outputs" ("completed_at_epoch_ms", "function_name") INCLUDE ("application_name");
