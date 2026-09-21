-- SQLite has no INCLUDE, so 115, 117 and 119 trail the key with application_name instead.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_in_flight_v2" ON "workflow_status" ("queue_name", "status", "priority", "created_at", "application_name") WHERE "status" IN ('ENQUEUED', 'PENDING');
