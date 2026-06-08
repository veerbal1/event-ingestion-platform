ALTER TABLE events
ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128);

UPDATE events
SET idempotency_key = id::TEXT
WHERE idempotency_key IS NULL;

ALTER TABLE events
ALTER COLUMN idempotency_key SET NOT NULL;
