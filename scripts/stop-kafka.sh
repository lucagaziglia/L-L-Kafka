#!/bin/bash
# Stop Kafka broker
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
source "$PROJECT_DIR/setup-env.sh"

KAFKA_DIR=$(ls -d "$PROJECT_DIR/kafka_2.13-"* 2>/dev/null | head -1)

if [ -z "$KAFKA_DIR" ]; then
    echo "❌ Kafka no encontrado!"
    exit 1
fi

echo "🛑 Deteniendo Kafka broker..."
"$KAFKA_DIR/bin/kafka-server-stop.sh"
echo "✅ Kafka detenido."
