-- Migration 107: The unclaimed half of the key pair started in migration 106.
-- Runs online because every pre-upgrade row is unclaimed, so the index covers
-- them all.

CREATE UNIQUE INDEX %s IF NOT EXISTS "uq_application_versions_unclaimed_version" ON %s."application_versions" ("version_name") WHERE "application_name" IS NULL;
