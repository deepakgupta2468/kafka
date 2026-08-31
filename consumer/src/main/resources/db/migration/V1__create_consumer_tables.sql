CREATE TABLE employees (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(255) NOT NULL
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE dead_letter_events (
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
