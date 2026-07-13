-- Migration 28 (CockroachDB variant): Drop the legacy
-- uq_workflow_status_queue_name_dedup_id constraint. CockroachDB exposes
-- the constraint as an index and rejects ALTER TABLE DROP CONSTRAINT for it,
-- so we DROP INDEX ... CASCADE instead. The constraint was superseded by
-- the partial unique index uq_workflow_status_dedup_id created in migration 27.

DROP INDEX IF EXISTS %s."uq_workflow_status_queue_name_dedup_id" CASCADE;
