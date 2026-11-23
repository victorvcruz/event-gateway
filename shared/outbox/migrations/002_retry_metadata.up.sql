ALTER TABLE {{ .TableName }}
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_error TEXT;

DROP INDEX IF EXISTS {{ .IndexPrefix }}_pending;
CREATE INDEX {{ .IndexPrefix }}_pending ON {{ .TableName }} (next_attempt_at, created_at)
  WHERE status = 'PENDING';
