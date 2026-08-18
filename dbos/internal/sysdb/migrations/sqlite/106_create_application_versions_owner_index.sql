-- With 107, the pair of keys replacing version_name's retiring global
-- uniqueness, unclaimed counting as its own owner.
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
    ON "application_versions" ("application_name", "version_name")
    WHERE "application_name" IS NOT NULL;
