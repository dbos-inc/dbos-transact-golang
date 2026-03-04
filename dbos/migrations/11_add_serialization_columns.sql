-- Migration 11: Add serialization column to workflow and event tables
-- Stores serialization format/metadata for workflow data.

ALTER TABLE %s.workflow_status ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE %s.notifications ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE %s.workflow_events ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE %s.workflow_events_history ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE %s.operation_outputs ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE %s.streams ADD COLUMN serialization TEXT DEFAULT NULL;
