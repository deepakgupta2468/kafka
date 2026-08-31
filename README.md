# Kafka employee events

Production-oriented employee event pipeline: the producer accepts create/delete requests, persists versioned events to a PostgreSQL outbox, and publishes them to Kafka. The consumer processes events idempotently and records terminal failures from the dead-letter topic.

## Architecture

- **producer** — REST API, transactional outbox, scheduled publisher
- **consumer** — Kafka listener, idempotent persistence, DLT handler
- **common-events** — shared versioned `EmployeeEvent` contract

Each service owns a separate PostgreSQL database. Schema changes are managed with Flyway.

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
- Producer: `kafka_exp1_producer`
- Consumer: `kafka_exp1_consumer`

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
