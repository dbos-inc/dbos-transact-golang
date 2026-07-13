-- Migration 27: Drop the original full index on workflow_status.status.
-- Replaced by the two partial indexes idx_workflow_status_pending and
-- idx_workflow_status_failed.

DROP INDEX %s IF EXISTS %s."workflow_status_status_index";
