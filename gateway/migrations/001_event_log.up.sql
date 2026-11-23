CREATE TABLE event_log (
  id             BIGSERIAL    PRIMARY KEY,
  event_id       UUID         NOT NULL,
  correlation_id VARCHAR(255) NOT NULL,
  event_type     VARCHAR(255) NOT NULL,
  direction      VARCHAR(12)  NOT NULL CHECK (direction IN ('RECEIVED', 'DISPATCHED')),
  service_id     VARCHAR(100),
  logged_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_event_log_correlation ON event_log (correlation_id, logged_at);
CREATE INDEX idx_event_log_type_direction ON event_log (event_type, direction, logged_at);
