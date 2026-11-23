DROP INDEX IF EXISTS {{ .IndexPrefix }}_pending;
CREATE INDEX {{ .IndexPrefix }}_pending ON {{ .TableName }} (created_at)
  WHERE status = 'PENDING';

ALTER TABLE {{ .TableName }}
  DROP COLUMN IF EXISTS last_error,
  DROP COLUMN IF EXISTS next_attempt_at;
