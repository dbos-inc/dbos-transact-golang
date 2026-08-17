-- NULL means unclaimed: any application may read and claim the row.
ALTER TABLE "workflow_status" ADD COLUMN "application_name" TEXT DEFAULT NULL;
