ALTER TABLE "workflow_status" ADD COLUMN "debounce_deadline_epoch_ms" INTEGER DEFAULT NULL;
ALTER TABLE "workflow_status" ADD COLUMN "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
