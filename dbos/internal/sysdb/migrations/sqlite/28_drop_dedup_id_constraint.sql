-- SQLite stores unique constraints as indexes (like CockroachDB), so we drop
-- the index that backs the legacy uq_workflow_status_queue_name_dedup_id
-- constraint. The replacement partial unique index was created by migration
-- 27 under a different name to avoid the collision.
DROP INDEX IF EXISTS "uq_workflow_status_queue_name_dedup_id";
