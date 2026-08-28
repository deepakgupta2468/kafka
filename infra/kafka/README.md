# Kafka topic provisioning

Provision topics before starting the producer or consumer. Applications only publish and consume; they do not create or modify topics.

```bash
chmod +x infra/kafka/create-topics.sh
./infra/kafka/create-topics.sh
```

The script creates `employees.v1` and `employees.v1.dlt` with five partitions and one replica by default. It automatically discovers the Homebrew Kafka CLI. Override values through `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_EMPLOYEE_TOPIC`, `KAFKA_TOPIC_PARTITIONS`, `KAFKA_TOPIC_REPLICATION_FACTOR`, and `KAFKA_BIN`.
