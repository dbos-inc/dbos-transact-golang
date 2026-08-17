-- Denormalized from the parent workflow so step observability filters
-- without a join.
ALTER TABLE "operation_outputs" ADD COLUMN "application_name" TEXT DEFAULT NULL;
