-- Migration 130 (Postgres-only tail): re-pin search_path on send_message.

ALTER FUNCTION %s.send_message(
    TEXT, JSON, TEXT, TEXT
) SET search_path = pg_catalog, pg_temp;
