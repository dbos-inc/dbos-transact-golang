-- Migration 42: Add debounce columns to workflow_status. A debounced workflow
-- is enqueued DELAYED holding its debounce key as its deduplication_id; each
-- bounce extends delay_until_epoch_ms, capped at debounce_deadline_epoch_ms.
-- is_debounced marks the deduplication ID as a debounce key to clear on the
-- DELAYED->ENQUEUED transition. ADD COLUMN with a constant default is
-- catalog-only, so no CONCURRENTLY is needed.

ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
ALTER TABLE %s."workflow_status" ADD COLUMN IF NOT EXISTS "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
