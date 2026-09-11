-- Nullable: SQLite cannot ADD COLUMN NOT NULL with a non-constant default; 112 rebuilds.

ALTER TABLE operation_outputs ADD COLUMN retention_timestamp INTEGER;
