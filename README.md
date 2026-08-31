# Kafka employee events

Production-oriented employee event pipeline: the producer accepts create/delete requests, persists versioned events to a PostgreSQL outbox, and publishes them to Kafka. The consumer processes events idempotently and records terminal failures from the dead-letter topic.

## Architecture

- **producer** — REST API, transactional outbox, scheduled publisher
- **consumer** — Kafka listener, idempotent persistence, DLT handler
- **common-events** — shared versioned `EmployeeEvent` contract

Each service owns a separate PostgreSQL database. Schema changes are managed with Flyway.

**Do not query the legacy `kafka_exp1` database for new events.** After the split, the producer writes to `kafka_exp1_producer` and the consumer writes employees to `kafka_exp1_consumer`.

## Databases

Host: `localhost`, port: `5433`, user: `postgres`.

| What you are checking | Database | Tables |
|-----------------------|----------|--------|
| Outbox publish status | `kafka_exp1_producer` | `outbox_events` |
| Persisted employees | `kafka_exp1_consumer` | `employees` |
| Consumer idempotency | `kafka_exp1_consumer` | `processed_events` |
| Dead-letter records | `kafka_exp1_consumer` | `dead_letter_events` |
| Old shared schema | `kafka_exp1` | leftover; ignore for new traffic |

Suggested DBeaver connections (create two, named exactly like this):

- **kafka-exp1-producer** → JDBC URL `jdbc:postgresql://localhost:5433/kafka_exp1_producer`
- **kafka-exp1-consumer** → JDBC URL `jdbc:postgresql://localhost:5433/kafka_exp1_consumer`

After `POST /employees`, confirm in **kafka-exp1-consumer** → `employees`, not in the producer DB or `kafka_exp1`.

```sql
-- consumer
SELECT * FROM employees ORDER BY id DESC LIMIT 10;
SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10;

-- producer
SELECT id, aggregate_key, status, published_at FROM outbox_events ORDER BY created_at DESC LIMIT 10;
```


## Quick start (Docker)

```bash
docker compose up --build
```

| Service | URL |
|---------|-----|
| Producer API | http://localhost:8080 |
| Producer health | http://localhost:9090/actuator/health |
| Consumer health | http://localhost:9091/actuator/health |
| Prometheus (producer) | http://localhost:9090/actuator/prometheus |
| Prometheus (consumer) | http://localhost:9091/actuator/prometheus |

Create an employee:

```bash
curl -X POST http://localhost:8080/employees \
  -H "Content-Type: application/json" \
  -d '{"id":101,"name":"Deepak Gupta","department":"Engineering"}'
```

Response (`202 Accepted`):

```json
{"eventId":"550e8400-e29b-41d4-a716-446655440000"}
```

## Local startup (without Docker)

Start PostgreSQL and Kafka, create databases, then provision topics:

```bash
psql -h localhost -p 5433 -U postgres -f infra/postgres/init-databases.sql
./infra/kafka/create-topics.sh
./gradlew :producer:bootRun
./gradlew :consumer:bootRun
```

Default databases:
- Producer: `kafka_exp1_producer` (outbox only)
- Consumer: `kafka_exp1_consumer` (employees, processed events, DLT)
- Legacy `kafka_exp1` is unused by the current apps


## Production deployment

Use the `prod` profile and provide required environment variables.

### Required environment variables

| Variable | Description |
|----------|-------------|
| `POSTGRES_URL` | JDBC URL for the service database |
| `POSTGRES_USERNAME` | Database username |
| `POSTGRES_PASSWORD` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers |
| `KAFKA_EMPLOYEE_TOPIC` | Employee topic name (default `employees.v1`) |
| `API_KEY` | Producer API key (prod profile enables auth) |

### Optional Kafka security

| Variable | Description |
|----------|-------------|
| `KAFKA_SECURITY_PROTOCOL` | e.g. `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | e.g. `PLAIN` |
| `KAFKA_SASL_JAAS_CONFIG` | JAAS config string |

### Deploy commands

Provision schema via Flyway on startup (`ddl-auto: validate`), or apply manually:

```bash
psql "$POSTGRES_URL" -f infra/postgres/create-schema.sql
```

```bash
SPRING_PROFILES_ACTIVE=prod API_KEY=your-secret ./gradlew :producer:bootRun
SPRING_PROFILES_ACTIVE=prod ./gradlew :consumer:bootRun
```

### Security

- Producer API requires `X-API-Key` header when `API_SECURITY_ENABLED=true` (enabled by default in `prod`)
- Actuator runs on a separate management port (`9090` producer, `9091` consumer)
- Restrict management ports to internal networks in Kubernetes

### Observability

Prometheus metrics:
- `employee_outbox_events_total` — tagged by `outcome` (`published`, `failed`)
- `employee_consumer_events_total` — tagged by `outcome` (`created`, `deleted`, `duplicate`, `dead_lettered`)

Health probes: `/actuator/health/liveness` and `/actuator/health/readiness`

Production logs use JSON format (Logstash encoder).

### Event contract

`EmployeeEvent` is a shared, versioned contract in `common-events`. The `employees.v1` topic must not contain legacy payloads when this version is deployed; migrate to a new topic or drain old records first.

## CI

GitHub Actions runs `./gradlew test` on push and pull requests.
