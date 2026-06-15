-- Unlike Postgres migration 40, this creates no index (SQLite has no GIN
-- equivalent) and stores attributes as TEXT rather than JSONB.
ALTER TABLE workflow_status ADD COLUMN "attributes" TEXT;
