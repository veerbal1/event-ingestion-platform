ALTER TABLE events
ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS dead_letter_reason TEXT;

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_status_valid;

ALTER TABLE events
ADD CONSTRAINT events_status_valid
CHECK (status IN ('accepted', 'processing', 'processed', 'failed', 'dead_lettered'));
