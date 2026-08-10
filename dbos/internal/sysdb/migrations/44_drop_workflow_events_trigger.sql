-- Migration 44: Drop the per-row workflow_events NOTIFY trigger installed by
-- migration 1, for the same reason as migration 43: SetEvent notifications are
-- now coalesced and pushed off the write path by the notifier loop.
--
-- The notifications trigger (DBOS.Send) is deliberately kept: messages can be
-- sent from anywhere, including processes with no notifier loop to flush them.

DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON %s.workflow_events;
DROP FUNCTION IF EXISTS %s.workflow_events_function();
