-- Migration 7: Add owner_xid column to workflow_status table

ALTER TABLE %s.workflow_status ADD COLUMN owner_xid TEXT DEFAULT NULL;
