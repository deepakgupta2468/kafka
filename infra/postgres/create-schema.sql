CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_key VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_published_at ON outbox_events (published_at);

CREATE TABLE IF NOT EXISTS employees (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    employee_id BIGINT,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    failure_reason TEXT,
    raw_payload TEXT
);

ALTER TABLE dead_letter_events ADD COLUMN IF NOT EXISTS raw_payload TEXT;
