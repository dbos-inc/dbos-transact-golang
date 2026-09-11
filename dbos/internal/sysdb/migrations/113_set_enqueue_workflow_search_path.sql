-- Migration 113 (Postgres-only tail): re-pin search_path on enqueue_workflow.

ALTER FUNCTION %s.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT, TEXT
) SET search_path = pg_catalog, pg_temp;
