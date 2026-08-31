CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    event_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_outbox_published_at ON outbox_events (published_at) WHERE status = 'PUBLISHED';
