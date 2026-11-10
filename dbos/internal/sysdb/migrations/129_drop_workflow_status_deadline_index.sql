-- Migration 129: Drop idx_workflow_status_deadline (migration 122),
-- superseded by idx_workflow_status_deadline_v2 (migration 128).

DROP INDEX %s IF EXISTS %s."idx_workflow_status_deadline";
