-- Migration 125: Index application version history for latest-version lookups.

CREATE INDEX IF NOT EXISTS "idx_application_versions_owner_timestamp" ON "application_versions" ("application_name", "version_timestamp" DESC);
