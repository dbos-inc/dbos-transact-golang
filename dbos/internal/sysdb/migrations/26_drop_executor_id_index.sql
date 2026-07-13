-- Migration 22: Drop the index on executor_id. The recovery query that used
-- this index no longer relies on it.

DROP INDEX %s IF EXISTS %s."workflow_status_executor_id_index";
