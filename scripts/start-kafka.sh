#!/bin/bash
# Start Kafka broker in KRaft mode
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
source "$PROJECT_DIR/setup-env.sh"

KAFKA_DIR=$(ls -d "$PROJECT_DIR/kafka_2.13-"* 2>/dev/null | head -1)

if [ -z "$KAFKA_DIR" ]; then
    echo "❌ Kafka no encontrado!"
    exit 1
fi

echo "🚀 Iniciando Kafka broker en modo KRaft..."
echo "   Bootstrap server: localhost:9092"
echo "   Presioná Ctrl+C para detener."
echo ""

"$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
