-- Migration 18: Drop the non-partial index on forked_from in preparation for
-- recreating it as a partial index (only on non-NULL values).

DROP INDEX %s IF EXISTS %s."idx_workflow_status_forked_from";
