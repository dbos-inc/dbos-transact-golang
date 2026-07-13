-- Migration 38: Replace enqueue_workflow with a signature that also accepts
-- authenticated_user, authenticated_roles, and delay_until_epoch_ms. A
-- non-NULL delay enqueues the workflow in the DELAYED state. The old 13-arg
-- function is dropped first so only the new signature remains.

DROP FUNCTION IF EXISTS %s.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
);

CREATE OR REPLACE FUNCTION %s.enqueue_workflow(
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
    priority INT4 DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL,
    authenticated_user TEXT DEFAULT NULL,
    authenticated_roles TEXT DEFAULT NULL,
    delay_until_epoch_ms BIGINT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INT4 := 0;
    v_priority INT4;
    v_status TEXT;
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
    IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
        RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;
    v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

    INSERT INTO %s.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization,
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms
    ) VALUES (
        v_workflow_id, v_status, v_serialized_inputs,
        workflow_name, class_name, config_name,
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json',
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms
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
