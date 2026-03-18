-- Migration 10: Add primary key to notifications table
-- An earlier version of DBOS had a bug where this table was created without a primary key.
-- The initial migration has been changed to create a key, and this migration creates the key
-- for existing applications.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class cl ON c.conrelid = cl.oid
        JOIN pg_namespace n ON cl.relnamespace = n.oid
        WHERE n.nspname = '%s'
          AND cl.relname = 'notifications'
          AND c.contype = 'p'
    ) THEN
        ALTER TABLE %s.notifications ADD PRIMARY KEY (message_uuid);
    END IF;
END $$;
