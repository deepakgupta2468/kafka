# PostgreSQL

Each service has its own database. New traffic never writes to the legacy `kafka_exp1` database.

| DBeaver connection name | Database | Used by | Tables |
|-------------------------|----------|---------|--------|
| kafka-exp1-producer | `kafka_exp1_producer` | producer | `outbox_events`, `flyway_schema_history` |
| kafka-exp1-consumer | `kafka_exp1_consumer` | consumer | `employees`, `processed_events`, `dead_letter_events`, `flyway_schema_history` |

Connection settings (local):

- Host: `localhost`
- Port: `5433`
- User: `postgres`
- Password: `admin`
- Driver: PostgreSQL

Create databases:

```bash
psql -h localhost -p 5433 -U postgres -f infra/postgres/init-databases.sql
```

Flyway applies schema on service startup. `create-schema.sql` is a manual fallback and also uses `\c` to switch between the two databases.
