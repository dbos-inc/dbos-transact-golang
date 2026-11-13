-- Migration 130: send_message also wakes a SUSPENDED recv on its topic.

CREATE OR REPLACE FUNCTION %s.send_message(
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
    UPDATE %s.workflow_status
    -- Status is tested in SET, not WHERE, so the planner cannot scan idx_workflow_status_suspended.
    SET delay_until_epoch_ms = CASE WHEN status = 'SUSPENDED' THEN 0 ELSE delay_until_epoch_ms END
    WHERE workflow_uuid = destination_id
        AND suspend_key = v_topic;
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'DBOS non-existent workflow'
            USING DETAIL = format('Destination workflow %%s does not exist', destination_id),
                ERRCODE = 'foreign_key_violation';
END;
$$ LANGUAGE plpgsql;
