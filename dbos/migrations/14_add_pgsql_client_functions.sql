-- Migration 14: Add plpgsql stored functions for direct SQL client access.

CREATE FUNCTION %s.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INTEGER DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INTEGER := 0;
    v_priority INTEGER;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;

    INSERT INTO %s.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        authenticated_user, assumed_role,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization
    ) VALUES (
        v_workflow_id, 'ENQUEUED', v_serialized_inputs,
        workflow_name, class_name, config_name,
        '', '',
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json'
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %%s with queue %%s and deduplication ID %%s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION %s.send_message(
    destination_id TEXT,
    message JSON,
    topic TEXT DEFAULT NULL,
    message_id TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_topic TEXT := COALESCE(topic, '__null__topic__');
    v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
BEGIN
    INSERT INTO %s.notifications (
        destination_uuid, topic, message, message_uuid, serialization
    ) VALUES (
        destination_id, v_topic, message, v_message_id, 'portable_json'
    )
    ON CONFLICT (message_uuid) DO NOTHING;
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'DBOS non-existent workflow'
            USING DETAIL = format('Destination workflow %%s does not exist', destination_id),
                ERRCODE = 'foreign_key_violation';
END;
$$ LANGUAGE plpgsql;
