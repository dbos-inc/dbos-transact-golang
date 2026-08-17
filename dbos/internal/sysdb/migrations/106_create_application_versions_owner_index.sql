-- Migration 106: With 107, the pair of keys replacing version_name's retiring
-- global uniqueness, unclaimed counting as its own owner. The old
-- version_name unique constraint may not be dropped until every SDK reaching
-- this database is past 107.

CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version" ON %s."application_versions" ("application_name", "version_name") WHERE "application_name" IS NOT NULL;
