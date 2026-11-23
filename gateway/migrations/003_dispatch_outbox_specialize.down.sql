DROP INDEX IF EXISTS idx_dispatch_outbox_ready;

ALTER TABLE dispatch_outbox DROP CONSTRAINT dispatch_outbox_pkey;
ALTER TABLE dispatch_outbox ADD PRIMARY KEY (event_id);

CREATE INDEX idx_dispatch_outbox_pending
  ON dispatch_outbox (created_at)
  WHERE status = 'PENDING';

ALTER TABLE dispatch_outbox
  DROP COLUMN IF EXISTS next_attempt_at,
  DROP COLUMN IF EXISTS occurred_at,
  DROP COLUMN IF EXISTS callback_url,
  DROP COLUMN IF EXISTS service_id;
