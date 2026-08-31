-- Producer DB: outbox only. Do not look here for employees.
CREATE DATABASE kafka_exp1_producer;

-- Consumer DB: employees, processed_events, dead_letter_events.
CREATE DATABASE kafka_exp1_consumer;

