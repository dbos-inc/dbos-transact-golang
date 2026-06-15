-- Migration 38 (Postgres-only tail): pin search_path on the new
-- enqueue_workflow overload, matching the hardening applied in migration 20.
-- Skipped on CockroachDB, which does not support ALTER FUNCTION ... SET.

ALTER FUNCTION %s.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT
) SET search_path = pg_catalog, pg_temp;
