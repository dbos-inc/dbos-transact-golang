CREATE INDEX IF NOT EXISTS "idx_operation_outputs_completed_at_function_name_v2" ON "operation_outputs" ("completed_at_epoch_ms", "function_name", "application_name");
