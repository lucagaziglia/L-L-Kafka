# 🚀 L&L - Apache Kafka

Repositorio de pruebas para aprender y experimentar con **Apache Kafka 4.3.1** (KRaft mode).

## 📋 Requisitos previos

- **Java 21+** (Eclipse Temurin recomendado)
- **macOS / Linux**

## ⚡ Instalación rápida

### 1. Descargar e instalar JDK 21

```bash
# macOS ARM64 (Apple Silicon)
curl -fsSL -o /tmp/jdk21.tar.gz \
  "https://api.adoptium.net/v3/binary/latest/21/ga/mac/aarch64/jdk/hotspot/normal/eclipse"
mkdir -p .tools
tar -xzf /tmp/jdk21.tar.gz -C .tools/
```

### 2. Descargar Apache Kafka 4.3.1

```bash
curl -fsSL -o /tmp/kafka.tgz \
  "https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz"
tar -xzf /tmp/kafka.tgz
```

### 3. Configurar el entorno

```bash
source setup-env.sh
```

## 🏃 Iniciar Kafka (KRaft mode - sin ZooKeeper)

```bash
# Formatear el storage (solo la primera vez)
./scripts/init-kafka.sh

# Iniciar el broker
./scripts/start-kafka.sh

# Detener el broker
./scripts/stop-kafka.sh
```

## 🧪 Pruebas básicas

```bash
# Crear un topic
kafka-topics.sh --create --topic test-topic --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Producir mensajes
kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092

# Consumir mensajes
kafka-console-consumer.sh --topic test-topic --from-beginning \
  --bootstrap-server localhost:9092
```

## 📁 Estructura del proyecto

```
L&L-Kafka/
├── kafka_2.13-4.3.1/     # Distribución binaria de Kafka (en .gitignore)
├── .tools/                # JDK y herramientas locales (en .gitignore)
├── scripts/               # Scripts de utilidad
│   ├── init-kafka.sh      # Inicialización KRaft
│   ├── start-kafka.sh     # Arranque del broker
│   └── stop-kafka.sh      # Parada del broker
├── config/                # Configuraciones custom (si aplican)
├── setup-env.sh           # Variables de entorno
├── .gitignore
└── README.md
```

## ☁️ Despliegue en IBM Cloud

Ver [docs/IBM_CLOUD_SETUP.md](docs/IBM_CLOUD_SETUP.md) para instrucciones sobre cómo instanciar una VM en IBM Cloud para hostear Kafka.

## 👥 Colaboradores

- Lucas Gaziglia
- Lucas Marino

---

> **Nota:** Kafka 4.x ya no requiere ZooKeeper. Usa KRaft mode nativamente para el manejo de metadatos.
