-- Migration 124: Index application version history for latest-version lookups.

CREATE INDEX %s IF NOT EXISTS "idx_application_versions_timestamp" ON %s."application_versions" ("version_timestamp" DESC);
