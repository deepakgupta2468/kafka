# Kafka employee events

The producer accepts employee create and delete requests, saves a versioned event to its PostgreSQL outbox, and publishes it to Kafka. The consumer processes the event exactly once from its database perspective and stores terminal failures from the dead-letter topic.

## Local startup

Start Kafka and PostgreSQL, create the `kafka_exp1` database, then provision Kafka topics:

```bash
./infra/kafka/create-topics.sh
./gradlew :producer:bootRun
./gradlew :consumer:bootRun --args='--server.port=8081'
```

The producer is available at `http://localhost:8080`; the consumer health endpoint is at `http://localhost:8081/actuator/health`.

## Production deployment

Use the `prod` profile and provide `POSTGRES_URL`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, and `KAFKA_EMPLOYEE_TOPIC`. Provision Kafka topics before deploying services; application instances do not create or modify Kafka topics.

Provision the PostgreSQL schema before deployment; production uses JPA validation and does not modify the schema:

```bash
psql "$POSTGRES_URL" -f infra/postgres/create-schema.sql
```

```bash
SPRING_PROFILES_ACTIVE=prod ./gradlew :producer:bootRun
SPRING_PROFILES_ACTIVE=prod ./gradlew :consumer:bootRun
```

Monitor the Prometheus endpoint at `/actuator/prometheus`. Important metrics are `employee_outbox_events_total` and `employee_consumer_events_total`, tagged by outcome.

`EmployeeEvent` is a shared, versioned contract in the `common-events` module. The existing `employees.v1` topic must not contain the older raw employee JSON payload when this version is deployed; migrate to a new topic or drain the old records first.
