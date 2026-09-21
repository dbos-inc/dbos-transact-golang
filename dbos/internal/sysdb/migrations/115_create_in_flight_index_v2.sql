-- Migration 115: Recreate the main dequeue index with application_name INCLUDEd.
-- INCLUDE, not a key column: app-scoped counts then run index-only, while the
-- planner still cannot BitmapOr on application_name. Supersedes
-- idx_workflow_status_in_flight (migration 32), dropped by migration 116.

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_in_flight_v2" ON %s."workflow_status" ("queue_name", "status", "priority", "created_at") INCLUDE ("application_name") WHERE "status" IN ('ENQUEUED', 'PENDING');
