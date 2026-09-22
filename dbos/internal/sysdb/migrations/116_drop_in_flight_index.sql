-- Migration 116: Drop the v1 dequeue index (migration 32), superseded by
-- idx_workflow_status_in_flight_v2 (migration 115).

DROP INDEX %s IF EXISTS %s."idx_workflow_status_in_flight";
