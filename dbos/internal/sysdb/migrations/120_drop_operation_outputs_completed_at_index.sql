-- Migration 120: Drop the v1 step-completion index (migration 19), superseded
-- by idx_operation_outputs_completed_at_function_name_v2 (migration 119).

DROP INDEX %s IF EXISTS %s."idx_operation_outputs_completed_at_function_name";
