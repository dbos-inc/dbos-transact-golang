-- Migration 28: Create a partial composite index used by the main dequeue
-- query. Indexes on (queue_name, status, priority, created_at) — the exact
-- predicates and sort order of the dequeue query — restricted to in-flight
-- (ENQUEUED or PENDING) workflows.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_in_flight" ON %s."workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN ('ENQUEUED', 'PENDING');
