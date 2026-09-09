-- Migration 108: Per-partition limits on queues. Any of these being set
-- partitions the queue; each applies per partition. ADD COLUMN with a constant
-- default is catalog-only, so no CONCURRENTLY is needed.

ALTER TABLE %s."queues" ADD COLUMN IF NOT EXISTS "partition_concurrency" INT4 DEFAULT NULL;
ALTER TABLE %s."queues" ADD COLUMN IF NOT EXISTS "partition_worker_concurrency" INT4 DEFAULT NULL;
ALTER TABLE %s."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_max" INT4 DEFAULT NULL;
ALTER TABLE %s."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_period_sec" DOUBLE PRECISION DEFAULT NULL;
