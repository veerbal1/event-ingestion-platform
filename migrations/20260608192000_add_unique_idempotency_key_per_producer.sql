WITH duplicate_events AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id, idempotency_key
            ORDER BY received_at, id
        ) AS duplicate_number
    FROM events
)
UPDATE events
SET idempotency_key =
    LEFT(idempotency_key, 80) || '-duplicate-' || events.id::TEXT
FROM duplicate_events
WHERE events.id = duplicate_events.id
    AND duplicate_events.duplicate_number > 1;

CREATE UNIQUE INDEX IF NOT EXISTS events_producer_id_idempotency_key_unique
ON events (producer_id, idempotency_key);
