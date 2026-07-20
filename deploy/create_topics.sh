#!/usr/bin/env bash
# Yerel Kafka container'ında gerekli topic'leri oluşturur.
# docker-compose up -d sonrası çalıştırın.

set -e

BOOTSTRAP="localhost:9092"

create_topic () {
  docker exec kafka kafka-topics --create \
    --if-not-exists \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$1" \
    --partitions "$2" \
    --replication-factor 1
}

create_topic earthquake.events.raw 3
create_topic earthquake.events.scored 6
create_topic district.risk.updated 6
create_topic aid.center.recommendations 3

docker exec kafka kafka-topics --list --bootstrap-server "$BOOTSTRAP"
