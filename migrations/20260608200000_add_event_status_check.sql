ALTER TABLE events
ADD CONSTRAINT events_status_valid
CHECK (status IN ('accepted', 'processing', 'processed', 'failed'));
