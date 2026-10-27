-- Migration 125: Index application version history for latest-version lookups.

CREATE INDEX %s IF NOT EXISTS "idx_application_versions_owner_timestamp" ON %s."application_versions" ("application_name", "version_timestamp" DESC);
