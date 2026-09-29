#!/bin/bash
# Initialize Kafka storage for KRaft mode (run only once)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
source "$PROJECT_DIR/setup-env.sh"

KAFKA_DIR=$(ls -d "$PROJECT_DIR/kafka_2.13-"* 2>/dev/null | head -1)

if [ -z "$KAFKA_DIR" ]; then
    echo "❌ Kafka no encontrado!"
    exit 1
fi

echo "=== Inicializando Kafka en modo KRaft ==="

# Generate cluster ID
KAFKA_CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
echo "📝 Cluster ID generado: $KAFKA_CLUSTER_ID"

# Save cluster ID for reference
echo "$KAFKA_CLUSTER_ID" > "$PROJECT_DIR/.cluster-id"

# Format storage
"$KAFKA_DIR/bin/kafka-storage.sh" format --standalone \
    -t "$KAFKA_CLUSTER_ID" \
    -c "$KAFKA_DIR/config/server.properties"

echo ""
echo "✅ Storage formateado correctamente."
echo "   Cluster ID guardado en .cluster-id"
echo "   Ejecutá ./scripts/start-kafka.sh para iniciar el broker."
