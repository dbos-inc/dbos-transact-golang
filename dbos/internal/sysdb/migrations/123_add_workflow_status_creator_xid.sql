-- Migration 123: Record the token of the insert that created the row, so
-- owner_xid can carry the token of the execution that currently owns it.

ALTER TABLE %s."workflow_status"
    ADD COLUMN IF NOT EXISTS "creator_xid" TEXT DEFAULT NULL;
