-- Records the token of the insert that created the row.

ALTER TABLE workflow_status ADD COLUMN "creator_xid" TEXT DEFAULT NULL;
