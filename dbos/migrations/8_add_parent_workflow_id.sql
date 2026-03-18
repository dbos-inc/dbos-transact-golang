-- Migration 8: Add parent_workflow_id column to workflow_status table
-- This enables tracking child workflow lineage for workflows spawned from within other workflows.

ALTER TABLE %s.workflow_status
ADD COLUMN parent_workflow_id TEXT DEFAULT NULL;

CREATE INDEX "idx_workflow_status_parent_workflow_id" ON %s."workflow_status" ("parent_workflow_id");
