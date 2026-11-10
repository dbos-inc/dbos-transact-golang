-- Migration 128: idx_workflow_status_deadline (migration 122) with a predicate
-- that also covers SUSPENDED workflows. Supersedes it; dropped by migration 129.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_deadline_v2" ON %s."workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED', 'SUSPENDED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
