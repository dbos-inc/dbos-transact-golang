-- Migration 40: Add a JSONB attributes column to workflow_status for arbitrary
-- user-supplied workflow metadata. ADD COLUMN with no default is catalog-only;
-- the partial index built in the same transaction covers zero rows, so no
-- CONCURRENTLY is needed. The index supports containment (@>) filters; on
-- CockroachDB, USING GIN creates an inverted index.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "attributes" JSONB;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_attributes" ON %s."workflow_status" USING GIN ("attributes") WHERE "attributes" IS NOT NULL;
