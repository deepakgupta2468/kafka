CREATE INDEX idx_processed_events_processed_at ON processed_events (processed_at);

ALTER TABLE dead_letter_events ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMPTZ;
