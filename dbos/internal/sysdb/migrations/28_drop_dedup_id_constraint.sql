-- Migration 24: Drop the original full unique constraint on
-- (queue_name, deduplication_id). The partial-index replacement
-- (uq_workflow_status_dedup_id) was created in migration 23.
--
-- This is a fast catalog operation, so CONCURRENTLY is not needed.
-- CockroachDB implements unique constraints as indexes and rejects
-- ALTER TABLE DROP CONSTRAINT for them; PostgreSQL rejects DROP INDEX
-- on a constraint-backed index. The runner picks the right statement
-- based on the dialect.

ALTER TABLE %s.workflow_status DROP CONSTRAINT IF EXISTS uq_workflow_status_queue_name_dedup_id;
