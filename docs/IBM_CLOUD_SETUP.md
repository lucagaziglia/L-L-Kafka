# ☁️ Configuración de Apache Kafka en IBM Cloud

Guía para instanciar una máquina virtual (VSI) en IBM Cloud VPC para hostear Apache Kafka.

## 🔄 Opciones de despliegue

### Opción A: Managed Service (Recomendado para producción)
**IBM Event Streams** es el servicio gestionado de Kafka en IBM Cloud. Automatiza escalado, parcheo, alta disponibilidad y monitoreo.

### Opción B: Self-hosted en VSI (Para aprendizaje y control total)
Esta guía cubre esta opción.

---

## 📋 Pre-requisitos

1. **Cuenta IBM Cloud** activa
2. **IBM Cloud CLI** instalado:
   ```bash
   curl -fsSL https://clis.cloud.ibm.com/install/osx | sh
   ```
3. **Plugin VPC**:
   ```bash
   ibmcloud plugin install vpc-infrastructure
   ```

## 🖥️ Especificaciones recomendadas para la VM

### Para pruebas/desarrollo (1 broker)

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| **CPU** | 2 vCPUs | 4 vCPUs |
| **RAM** | 4 GB | 8 GB |
| **Disco** | 50 GB SSD | 100 GB SSD (alta IOPS) |
| **OS** | Ubuntu 22.04 LTS | Ubuntu 22.04 LTS |
| **Perfil IBM** | `bx2-2x8` | `bx2-4x16` |

### Para producción (mínimo 3 brokers)

| Recurso | Por broker |
|---------|-----------|
| **CPU** | 8+ vCPUs |
| **RAM** | 32-64 GB |
| **Disco** | 500 GB+ SSD (IOPS alto, separado del OS) |
| **Red** | Alta bandwidth, baja latencia |
| **Perfil IBM** | `mx2-8x64` o `bx2-8x32` |

### Estimación de costos (pay-as-you-go)

| Perfil | vCPU | RAM | Costo aprox/mes |
|--------|------|-----|-----------------|
| `bx2-2x8` | 2 | 8 GB | ~$60-80 USD |
| `bx2-4x16` | 4 | 16 GB | ~$120-160 USD |
| `bx2-8x32` | 8 | 32 GB | ~$240-320 USD |
| `mx2-8x64` | 8 | 64 GB | ~$400-500 USD |

> **Tip:** Usar reservas (1 o 3 años) puede dar hasta 60% de descuento. Usar el [IBM Cloud Cost Estimator](https://cloud.ibm.com/estimator) para cotización exacta.

---

## 🚀 Pasos de despliegue con CLI

### 1. Login

```bash
ibmcloud login --sso
ibmcloud target -r us-south  # o la región más cercana
```

### 2. Crear VPC

```bash
ibmcloud is vpc-create kafka-vpc
```

### 3. Crear Subnet

```bash
# Obtener la VPC ID
VPC_ID=$(ibmcloud is vpcs --output json | jq -r '.[] | select(.name=="kafka-vpc") | .id')

# Crear subnet
ibmcloud is subnet-create kafka-subnet $VPC_ID --zone us-south-1 \
  --ipv4-cidr-block 10.240.0.0/24
```

### 4. Crear SSH Key

```bash
# Si no tenés una key SSH, generala:
ssh-keygen -t ed25519 -C "kafka-ibm" -f ~/.ssh/kafka-ibm

# Registrar la key en IBM Cloud
ibmcloud is key-create kafka-key @~/.ssh/kafka-ibm.pub
```

### 5. Crear Security Group Rules

```bash
# Obtener el security group del VPC
SG_ID=$(ibmcloud is vpcs --output json | jq -r '.[] | select(.name=="kafka-vpc") | .default_security_group.id')

# Permitir SSH (puerto 22)
ibmcloud is sg-rulec $SG_ID inbound tcp --port-min 22 --port-max 22

# Permitir Kafka broker (puerto 9092)
ibmcloud is sg-rulec $SG_ID inbound tcp --port-min 9092 --port-max 9092

# Permitir KRaft controller (puerto 9093)
ibmcloud is sg-rulec $SG_ID inbound tcp --port-min 9093 --port-max 9093
```

### 6. Crear la VM (VSI)

```bash
# Obtener IDs necesarios
SUBNET_ID=$(ibmcloud is subnets --output json | jq -r '.[] | select(.name=="kafka-subnet") | .id')
KEY_ID=$(ibmcloud is keys --output json | jq -r '.[] | select(.name=="kafka-key") | .id')

# Buscar la imagen de Ubuntu 22.04
IMAGE_ID=$(ibmcloud is images --output json | jq -r '[.[] | select(.name | contains("ubuntu-22-04")) | select(.status=="available")] | sort_by(.created_at) | last | .id')

# Crear la instancia
ibmcloud is instance-create kafka-broker-1 $VPC_ID us-south-1 bx2-4x16 \
  $SUBNET_ID --image-id $IMAGE_ID --key-ids $KEY_ID
```

### 7. Asignar IP flotante

```bash
# Obtener el ID de la interfaz de red
NIC_ID=$(ibmcloud is instance kafka-broker-1 --output json | jq -r '.primary_network_interface.id')

# Asignar floating IP
ibmcloud is floating-ip-reserve kafka-fip --nic-id $NIC_ID
```

### 8. Conectarse e instalar Kafka

```bash
# Obtener la IP flotante
FLOAT_IP=$(ibmcloud is floating-ips --output json | jq -r '.[] | select(.name=="kafka-fip") | .address')

# Conectarse
ssh -i ~/.ssh/kafka-ibm root@$FLOAT_IP
```

Una vez dentro de la VM:

```bash
# Actualizar sistema
apt update && apt upgrade -y

# Instalar Java 21
apt install -y openjdk-21-jdk

# Descargar Kafka
cd /opt
curl -fsSL -o kafka.tgz \
  "https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz"
tar -xzf kafka.tgz
mv kafka_2.13-4.3.1 kafka

# Configurar
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$KAFKA_HOME/bin

# Inicializar KRaft
KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)
kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /opt/kafka/config/server.properties

# Iniciar Kafka como servicio (ver más abajo)
```

### 9. Crear servicio systemd

```bash
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka Broker
After=network.target

[Service]
Type=simple
User=kafka
Environment=JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
Environment=KAFKA_HOME=/opt/kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=128000

[Install]
WantedBy=multi-user.target
EOF

# Crear usuario kafka
useradd -r -s /bin/false kafka
chown -R kafka:kafka /opt/kafka

# Habilitar e iniciar
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
```

---

## 🔧 Optimizaciones del OS para Kafka

```bash
# Aumentar file descriptors
echo "* soft nofile 128000" >> /etc/security/limits.conf
echo "* hard nofile 128000" >> /etc/security/limits.conf

# Optimizar memoria virtual
echo "vm.swappiness=1" >> /etc/sysctl.conf
echo "vm.dirty_background_ratio=5" >> /etc/sysctl.conf
echo "vm.dirty_ratio=60" >> /etc/sysctl.conf
sysctl -p
```

## 🔒 Seguridad

- Habilitar **SSL/TLS** para cifrado en tránsito (considerar ~20-30% overhead de CPU)
- Usar **SASL (SCRAM-SHA-256)** para autenticación
- Configurar **ACLs** para autorización por topic
- Mantener el Security Group restrictivo (solo IPs conocidas)

## 📊 Monitoreo

- Usar **JMX Exporter** + **Prometheus** + **Grafana** para métricas
- Métricas clave: consumer lag, broker throughput, disk usage, request latency
- IBM Cloud Monitoring integrado para alertas
