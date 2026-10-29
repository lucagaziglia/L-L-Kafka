# Práctica base · Particiones, claves y grupos

**Duración:** 90 minutos. Hacer antes del primer hands on de Flink. Objetivo: observar cómo Kafka reparte datos y trabajo, usando un sandbox local.

## 1 · Broker aislado

Desde `estudio-eventos/`, terminal 1:

```bash
python3 scripts/kafka_demo.py --serve-only
```

En cada terminal adicional, abrir bash y cargar las herramientas de la carpeta padre (el script existente está escrito para bash):

```bash
bash
source ../setup-env.sh
```

## 2 · Topic con tres particiones y claves

Terminal 2:

```bash
kafka-topics.sh --bootstrap-server 127.0.0.1:19092 --create \
  --topic ll.learning.keys.v1 --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server 127.0.0.1:19092 --describe --topic ll.learning.keys.v1
kafka-console-producer.sh --bootstrap-server 127.0.0.1:19092 \
  --topic ll.learning.keys.v1 --property parse.key=true --property key.separator=:
```

Escribir estas líneas y terminar con Ctrl+C:

```text
cliente-a:{"sequence":1,"event":"view_item"}
cliente-b:{"sequence":1,"event":"view_item"}
cliente-a:{"sequence":2,"event":"begin_checkout"}
cliente-b:{"sequence":2,"event":"purchase"}
cliente-c:{"sequence":1,"event":"page_view"}
```

El separador `:` divide key y value; el resto de los `:` dentro del JSON queda en el value.

## 3 · Ver clave, partición y posición

Terminal 3:

```bash
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:19092 \
  --topic ll.learning.keys.v1 --from-beginning --group ll-learning-a \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Observá si los eventos de `cliente-a` van a la misma partición, y si sus offsets crecen. Diferentes claves pueden caer en la misma partición. El orden global impreso entre particiones no tiene por qué coincidir con el orden que escribiste.

## 4 · Repartir trabajo o tener otro consumidor independiente

Terminal 4: repetir el consumer con el **mismo grupo** `ll-learning-a`. Publicar más mensajes desde terminal 2. Ahora las particiones se reparten entre los miembros del grupo. Con tres particiones, un cuarto consumidor no aumenta el paralelismo útil de lectura de ese topic.

Terminal 5: usar **otro grupo**, `ll-learning-b`, con `--from-beginning`. Ese grupo tiene sus propios offsets y puede leer el mismo historial. `--from-beginning` afecta el inicio cuando no existen offsets válidos; no borra commits existentes de un grupo que ya usaste.

Inspeccionar:

```bash
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:19092 --describe --group ll-learning-a
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:19092 --describe --group ll-learning-b
```

## 5 · Explicar y registrar

Detener un consumidor del grupo A, publicar más eventos, observar rebalance/lag y reanudarlo. Comparar con el grupo B. Explicar qué consumidor corresponde a “BI” y cuál a “motor de decisiones” si cada aplicación necesita procesar el historial de forma independiente.

Entregable: descripción del topic, tres ejemplos key/partition/offset, asignación por grupo y una explicación de replay. El sandbox es de un broker y no demuestra tolerancia a caída de un broker. Para replicación/HA se necesita otro ensayo con varios brokers.

Cerrar consumidores y usar Ctrl+C en la terminal 1 para detener sólo el sandbox del ejercicio.
