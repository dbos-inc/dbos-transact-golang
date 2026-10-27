-- Migration 124: Index application version history for latest-version lookups.

CREATE INDEX IF NOT EXISTS "idx_application_versions_timestamp" ON "application_versions" ("version_timestamp" DESC);
