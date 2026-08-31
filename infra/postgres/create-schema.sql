-- Producer database schema (managed by Flyway in the producer service).
-- DBeaver: connect as kafka-exp1-producer → database kafka_exp1_producer.
-- Tables here: outbox_events only. Employees are NOT stored in this database.
\c kafka_exp1_producer

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_key VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

-- Consumer database schema (managed by Flyway in the consumer service).
-- DBeaver: connect as kafka-exp1-consumer → database kafka_exp1_consumer.
-- After POST /employees, query employees here (not kafka_exp1 or kafka_exp1_producer).
\c kafka_exp1_consumer

CREATE TABLE IF NOT EXISTS employees (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    employee_id BIGINT,
    received_at TIMESTAMPTZ NOT NULL,
    failure_reason TEXT,
    raw_payload TEXT,
    source_topic VARCHAR(255),
    source_partition INT,
    source_offset BIGINT,
    exception_message TEXT
);
