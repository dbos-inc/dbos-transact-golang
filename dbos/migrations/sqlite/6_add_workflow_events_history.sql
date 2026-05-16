CREATE TABLE workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE streams ADD COLUMN function_id INTEGER NOT NULL DEFAULT 0;
