ALTER TABLE dispatch_outbox
  ADD COLUMN service_id      VARCHAR(100) NOT NULL,
  ADD COLUMN callback_url    TEXT         NOT NULL,
  ADD COLUMN occurred_at     TIMESTAMPTZ  NOT NULL,
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE dispatch_outbox DROP CONSTRAINT dispatch_outbox_pkey;
ALTER TABLE dispatch_outbox ADD PRIMARY KEY (event_id, service_id);

DROP INDEX idx_dispatch_outbox_pending;

CREATE INDEX idx_dispatch_outbox_ready
  ON dispatch_outbox (next_attempt_at)
  WHERE status = 'PENDING';
