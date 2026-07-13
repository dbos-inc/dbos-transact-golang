-- Migration 10 (CockroachDB variant): probe whether the notifications primary
-- key already exists. CockroachDB does not support the DO block used by the
-- Postgres migration file, so the runner emulates the same idempotence by
-- running this check, then conditionally executing the ALTER from
-- 10_add_notifications_pkey_cockroach.sql.

SELECT 1 FROM pg_constraint c
JOIN pg_class cl ON c.conrelid = cl.oid
JOIN pg_namespace n ON cl.relnamespace = n.oid
WHERE n.nspname = $1
  AND cl.relname = 'notifications'
  AND c.contype = 'p';
