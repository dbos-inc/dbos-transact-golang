-- Migration 13: Add application_versions table for version tracking.

CREATE TABLE %s.application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint
);
