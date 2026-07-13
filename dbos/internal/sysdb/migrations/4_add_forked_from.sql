-- Migration 4: Add forked_from column to workflow_status table
-- This enables tracking workflow fork lineage

ALTER TABLE %s.workflow_status
ADD COLUMN forked_from TEXT;

CREATE INDEX "idx_workflow_status_forked_from" ON %s."workflow_status" ("forked_from");

