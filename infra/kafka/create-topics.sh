#!/usr/bin/env bash
set -euo pipefail

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-5}"
KAFKA_TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
KAFKA_EMPLOYEE_TOPIC="${KAFKA_EMPLOYEE_TOPIC:-employees.v1}"

if [[ -z "${KAFKA_BIN:-}" ]]; then
  if command -v kafka-topics >/dev/null 2>&1; then
    KAFKA_BIN="kafka-topics"
  elif command -v kafka-topics.sh >/dev/null 2>&1; then
    KAFKA_BIN="kafka-topics.sh"
  elif command -v brew >/dev/null 2>&1 && [[ -x "$(brew --prefix kafka)/bin/kafka-topics" ]]; then
    KAFKA_BIN="$(brew --prefix kafka)/bin/kafka-topics"
  else
    echo "Kafka topic CLI not found. Set KAFKA_BIN to the kafka-topics executable." >&2
    exit 1
  fi
fi

"${KAFKA_BIN}" --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --create --if-not-exists \
  --topic "${KAFKA_EMPLOYEE_TOPIC}" \
  --partitions "${KAFKA_TOPIC_PARTITIONS}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}"

"${KAFKA_BIN}" --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --create --if-not-exists \
  --topic "${KAFKA_EMPLOYEE_TOPIC}.dlt" \
  --partitions "${KAFKA_TOPIC_PARTITIONS}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}"
