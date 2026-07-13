CREATE TABLE application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
