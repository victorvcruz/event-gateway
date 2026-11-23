DROP INDEX IF EXISTS uq_dead_letter_kafka_source;

ALTER TABLE dead_letter
  DROP COLUMN IF EXISTS failure_code,
  DROP COLUMN IF EXISTS source_offset,
  DROP COLUMN IF EXISTS source_partition,
  DROP COLUMN IF EXISTS source_topic,
  DROP COLUMN IF EXISTS raw_payload;

UPDATE dead_letter SET payload = '{}'::jsonb WHERE payload IS NULL;
ALTER TABLE dead_letter ALTER COLUMN payload SET NOT NULL;
