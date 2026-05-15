-- Migration 10 (CockroachDB variant): add the notifications primary key.
-- Only run if 10_check_notifications_pkey_cockroach.sql reported no existing
-- primary key. CockroachDB's lack of DO-block support means this idempotence
-- check is done by the runner rather than the SQL itself.

ALTER TABLE %s.notifications ADD CONSTRAINT notifications_pkey PRIMARY KEY (message_uuid);
