ALTER TABLE "queues" ADD COLUMN "partition_concurrency" INTEGER DEFAULT NULL;
ALTER TABLE "queues" ADD COLUMN "partition_worker_concurrency" INTEGER DEFAULT NULL;
ALTER TABLE "queues" ADD COLUMN "partition_rate_limit_max" INTEGER DEFAULT NULL;
ALTER TABLE "queues" ADD COLUMN "partition_rate_limit_period_sec" REAL DEFAULT NULL;
