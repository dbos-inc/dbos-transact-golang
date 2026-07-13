-- Migration 31: Drop the original queue-status-started index. Superseded by
-- the partial in-flight index (idx_workflow_status_in_flight) and the partial
-- rate-limited index (idx_workflow_status_rate_limited).

DROP INDEX %s IF EXISTS %s."idx_workflow_status_queue_status_started";
