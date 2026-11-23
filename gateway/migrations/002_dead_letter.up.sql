CREATE TABLE dead_letter (
  id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id       UUID         NOT NULL,
  event_type     VARCHAR(255) NOT NULL,
  service_id     VARCHAR(100) NOT NULL,
  payload        JSONB        NOT NULL,
  failure_reason TEXT,
  failed_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_dead_letter_service ON dead_letter (service_id, failed_at);
