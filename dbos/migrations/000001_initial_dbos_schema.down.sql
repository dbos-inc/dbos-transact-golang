-- 001_initial_dbos_schema.down.sql

-- Drop triggers first
DROP TRIGGER IF EXISTS dbos_notifications_trigger ON dbos.notifications;
DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON dbos.workflow_events;

-- Drop function
DROP FUNCTION IF EXISTS dbos.notifications_function();
DROP FUNCTION IF EXISTS dbos.workflow_events_function();

-- Drop tables in reverse order to respect foreign key constraints
DROP TABLE IF EXISTS dbos.workflow_events;
DROP TABLE IF EXISTS dbos.notifications;
DROP TABLE IF EXISTS dbos.operation_outputs;
DROP TABLE IF EXISTS dbos.workflow_status;

-- Drop the schema (only if empty)
DROP SCHEMA IF EXISTS dbos;