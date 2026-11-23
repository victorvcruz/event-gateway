ALTER TABLE dead_letter
  ALTER COLUMN payload DROP NOT NULL,
  ADD COLUMN raw_payload BYTEA,
  ADD COLUMN source_topic VARCHAR(255),
  ADD COLUMN source_partition INTEGER,
  ADD COLUMN source_offset BIGINT,
  ADD COLUMN failure_code VARCHAR(100);

CREATE UNIQUE INDEX uq_dead_letter_kafka_source
  ON dead_letter (source_topic, source_partition, source_offset)
  WHERE source_topic IS NOT NULL
    AND source_partition IS NOT NULL
    AND source_offset IS NOT NULL;
