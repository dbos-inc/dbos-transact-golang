-- Migration 29: Add a rate_limited column to workflow_status. Workflows
-- dequeued from a queue with a rate limiter are marked rate_limited = TRUE
-- so the rate-limiter count can use a partial index restricted to those rows
-- (see idx_workflow_status_rate_limited in migration 30).
--
-- ALTER TABLE ADD COLUMN with a constant default is fast on Postgres because
-- it is a catalog-only update (attmissingval), so CONCURRENTLY is not needed.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE;
