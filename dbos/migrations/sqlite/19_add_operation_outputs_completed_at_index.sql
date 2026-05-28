CREATE INDEX "idx_operation_outputs_completed_at_function_name"
    ON "operation_outputs" ("completed_at_epoch_ms", "function_name");
