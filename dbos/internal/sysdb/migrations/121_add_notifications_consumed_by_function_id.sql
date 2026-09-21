-- Migration 121: Record which recv consumed a notification, so a rewind can
-- delete the messages the discarded run took delivery of.

ALTER TABLE %s."notifications"
    ADD COLUMN IF NOT EXISTS "consumed_by_function_id" INTEGER;
