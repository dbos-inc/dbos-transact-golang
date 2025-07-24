-- 001_initial_dbos_schema.up.sql

-- Create the dbos schema
CREATE SCHEMA IF NOT EXISTS dbos;

-- Enable uuid extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create workflow_status table
CREATE TABLE dbos.workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    application_version TEXT,
    application_id TEXT,
    class_name VARCHAR(255) DEFAULT NULL,
    config_name VARCHAR(255) DEFAULT NULL,
    recovery_attempts BIGINT DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    started_at_epoch_ms BIGINT,
    deduplication_id TEXT,
    inputs TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);

-- Create indexes for workflow_status
CREATE INDEX workflow_status_created_at_index ON dbos.workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON dbos.workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON dbos.workflow_status (status);

-- Create unique constraint for queue_name and deduplication_id
ALTER TABLE dbos.workflow_status 
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id 
UNIQUE (queue_name, deduplication_id);

-- Create operation_outputs table
CREATE TABLE dbos.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE dbos.notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(), -- Built-in function
    FOREIGN KEY (destination_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
-- Create index for notifications
CREATE INDEX idx_workflow_topic ON dbos.notifications (destination_uuid, topic);

-- Create notification function
CREATE OR REPLACE FUNCTION dbos.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create notification trigger
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON dbos.notifications
FOR EACH ROW EXECUTE FUNCTION dbos.notifications_function();

-- Create workflow_events table
CREATE TABLE dbos.workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- Create events function
CREATE OR REPLACE FUNCTION dbos.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create events trigger
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON dbos.workflow_events
FOR EACH ROW EXECUTE FUNCTION dbos.workflow_events_function();
