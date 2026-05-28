ALTER TABLE "workflow_status" ADD COLUMN "delay_until_epoch_ms" INTEGER DEFAULT NULL;
CREATE INDEX "idx_workflow_status_delayed" ON "workflow_status" ("delay_until_epoch_ms")
    WHERE status = 'DELAYED';
