-- Migration 19: Index operation_outputs on (completed_at_epoch_ms, function_name)
-- to speed up queries that filter or sort step outputs by completion time.

CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON %s."operation_outputs" ("completed_at_epoch_ms", "function_name");
