-- Migration 18: Add was_forked_from column to workflow_status. Marks workflows
-- that have been forked at least once, so the fork query can avoid scanning
-- rows that have never been a fork source.

ALTER TABLE %s."workflow_status" ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE;
