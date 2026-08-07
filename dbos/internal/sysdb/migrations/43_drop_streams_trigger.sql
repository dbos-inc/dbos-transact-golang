-- Migration 43: Drop the per-row streams NOTIFY trigger installed by migration
-- 39. A notifying commit takes a global lock on the async notification queue,
-- so notifying from inside the write transaction serializes stream writes.
-- Notifications are now coalesced in memory and pushed off the write path by
-- the notifier loop. Postgres-only, matching migration 39.

DROP TRIGGER IF EXISTS dbos_streams_trigger ON %s.streams;
DROP FUNCTION IF EXISTS %s.streams_function();
