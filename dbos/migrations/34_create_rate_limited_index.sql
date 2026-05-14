-- Migration 30: Create a partial index used by the rate-limiter count query
-- to quickly find recent workflows in a rate-limited queue. The index is only
-- maintained for workflows actually dequeued under a rate limiter.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_rate_limited" ON %s."workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE;
