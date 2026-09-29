#!/bin/bash
# Setup environment variables for Kafka development
# Usage: source setup-env.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# JDK
JDK_DIR=$(ls -d "$SCRIPT_DIR/.tools/jdk-"* 2>/dev/null | head -1)
if [ -n "$JDK_DIR" ]; then
    export JAVA_HOME="$JDK_DIR/Contents/Home"
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "✅ JAVA_HOME=$JAVA_HOME"
    java -version 2>&1 | head -1
else
    echo "⚠️  JDK no encontrado en .tools/. Descargalo con:"
    echo '  curl -fsSL -o /tmp/jdk21.tar.gz "https://api.adoptium.net/v3/binary/latest/21/ga/mac/aarch64/jdk/hotspot/normal/eclipse"'
    echo '  mkdir -p .tools && tar -xzf /tmp/jdk21.tar.gz -C .tools/'
fi

# Kafka
KAFKA_DIR=$(ls -d "$SCRIPT_DIR/kafka_2.13-"* 2>/dev/null | head -1)
if [ -n "$KAFKA_DIR" ]; then
    export KAFKA_HOME="$KAFKA_DIR"
    export PATH="$KAFKA_HOME/bin:$PATH"
    echo "✅ KAFKA_HOME=$KAFKA_HOME"
else
    echo "⚠️  Kafka no encontrado. Descargalo con:"
    echo '  curl -fsSL -o /tmp/kafka.tgz "https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz"'
    echo '  tar -xzf /tmp/kafka.tgz'
fi

# gh CLI (optional)
GH_DIR="$SCRIPT_DIR/.tools/gh/bin"
if [ -d "$GH_DIR" ]; then
    export PATH="$GH_DIR:$PATH"
    echo "✅ gh CLI disponible"
fi

echo ""
echo "🎉 Entorno listo. Podés usar los comandos de Kafka directamente."
