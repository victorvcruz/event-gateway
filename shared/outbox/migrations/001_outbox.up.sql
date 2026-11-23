CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE {{ .TableName }} (
  event_id       UUID         PRIMARY KEY,
  event_type     VARCHAR(255) NOT NULL,
  payload        JSONB        NOT NULL,
  correlation_id VARCHAR(255),
  status         VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                              CHECK (status IN ('PENDING', 'SENT', 'DEAD')),
  retry_count    INT          NOT NULL DEFAULT 0,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ
);

CREATE INDEX {{ .IndexPrefix }}_pending ON {{ .TableName }} (created_at)
  WHERE status = 'PENDING';
