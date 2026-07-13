-- Migration 20: Pin search_path on the plpgsql functions installed by the
-- earlier migrations. Without this, a function's search_path resolves at
-- call time against whatever the caller has set, which lets an attacker with
-- CREATE-on-schema privileges shadow built-ins. Setting it to
-- pg_catalog, pg_temp ensures references resolve only to the system catalog.
--
-- This migration is skipped on CockroachDB, which does not support
-- ALTER FUNCTION ... SET. The runner passes the empty string in that case.

ALTER FUNCTION %s.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION %s.send_message(
    TEXT, JSON, TEXT, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION %s.notifications_function() SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION %s.workflow_events_function() SET search_path = pg_catalog, pg_temp;
