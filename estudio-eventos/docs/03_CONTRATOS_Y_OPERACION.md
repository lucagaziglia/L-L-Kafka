# Contratos, fallos y operación

## Contratos de los ejercicios

| Flujo | Key Kafka / correlación | Campos principales | Regla |
|---|---|---|---|
| Riesgo observado | entity_id / event_id | event_type, event_time_ms, payload | No incluye label; mismo event_id en reintentos |
| Etiqueta de riesgo | misma entidad / observation_id | label, event_time_ms | Llega después; no se aprende dos veces de la misma observación |
| Decisión | entidad / decision_id | observation_id, model_version, probability, threshold, action | Identidad determinista para un replay con modelo congelado |
| Marketing canónico | client_id + session_id / event_id | event, timestamp_ms, campaign, consentimiento | Allowlist, campos válidos y permiso de analytics |
| Compra backend | transaction_id / event_id | value, currency, fuente confiable | Dedup de negocio aunque el tracking tenga otro event_id |
| Acción marketing | identidad compuesta de sesión / action_id | action, reason o cancels | Elegibilidad y eventual compensación; no hay envío real |

El [JSON Schema](../examples/risk-observation.schema.json) documenta la observación online. El histórico de entrenamiento tiene una label adicional y es un contrato diferente. El código valida las features utilizadas y la normalización de marketing; no instala un validador JSON Schema ni un Schema Registry.

Las sesiones del motor se distinguen por `(client_id, session_id)`. En los IDs de acción del fixture se concatenan con `:`; en un contrato general usá una codificación no ambigua o hash del par, con política explícita de privacidad. Los session_id de Analytics no deben tratarse como globalmente únicos.

## Evolución de esquemas

Agregar un campo opcional con default puede ser compatible, según formato y política. Renombrar, cambiar unidades, tipo o semántica puede romper consumidores aunque el JSON siga siendo parseable. Definir owner, versión, compatibilidad, período de deprecación y ejemplos válidos/inválidos. Una versión en el nombre no sustituye la negociación del contrato.

En producción elegir JSON Schema, Avro o Protobuf y validar con una política del registry. Mantener las credenciales y configuraciones reales fuera de los fixtures.

## Garantías: dónde terminan

Kafka preserva orden por partición. La key ayuda a ubicar eventos relacionados; no garantiza orden entre topics distintos. Si las labels viajan por otro topic, pueden llegar antes de que el consumidor haya procesado la observación. Este motor falla explícitamente en ese caso; una implementación continua podría bufferizar con TTL y monitorear el faltante.

El productor idempotente reduce duplicados por reintentos de transporte. No elimina dos eventos creados por la aplicación con distinto ID para la misma compra. Exactly-once en un sink Kafka de Flink necesita checkpoints, transacciones/configuración y consumidores compatibles. No cubre automáticamente sistemas externos ni acciones humanas.

El script Kafka usa un broker y replication factor 1, estado Python en memoria y pruebas con streams finitos. No ofrece HA, persistencia de dedup entre reinicios ni coordinación transaccional de offsets/decisiones. Su objetivo verificable es transporte real y lógica reproducible, sin infraestructura de producción implícita.

## Escenarios de fallo para las semanas 3, 8 y 15

| Experimento | Qué observar | Criterio |
|---|---|---|
| Pausar consumidor, seguir produciendo | Lag crece y luego baja | La recuperación no pierde observaciones según IDs de input |
| Agregar consumidor del mismo grupo | Rebalance/asignación | No recibe una copia adicional de todo como otro grupo independiente |
| Reenviar evento/compra | ID y transacción | No duplicar decisión o ingreso dentro de la política de dedup |
| Matar worker de Flink | Checkpoint, estado, reinicio, offsets | Documentar estado restaurado; una ejecución desde cero es otra prueba |
| Reducir capacidad del sink | Backpressure y duración de checkpoint | Alertar antes de agotar retención y capacidad de estado |
| Detener una partición fuente | Watermark global puede detenerse | Investigar idle timeout y su tradeoff con tardanza |
| Compra después del watermark | Métrica cerrada y compensación | Corregir estado operacional y reconciliar histórico según contrato |
| Modelo nuevo con mismo input | Versiones y scores | Reproducir qué modelo y política emitieron cada decisión |

Registrar estas pruebas en un entorno propio de laboratorio. El informe de validación de entrega sólo declara las pruebas efectivamente ejecutadas; las caídas de worker/restore y carga son ejercicios de estudio.

## SLO para el capstone: propuestas que hay que medir

Objetivo inicial orientativo: p95 de recepción en collector a emisión de decisión <2 s con la carga del ensayo, lag estable y cero acciones duplicadas tras reintentos controlados. Es una meta de diseño, no un resultado medido en esta entrega.

Separar `event_time` (lo ocurrido) de `ingested_at` (recepción) y `decision_at` (emisión). Para duración dentro del proceso usar reloj monotónico; para comparar hosts, sincronización y semántica explícitas. No calcular “latencia de Kafka” restando timestamps sintéticos de 2026 al reloj de una ejecución posterior.

Monitorear throughput, consumer lag, errores de schema, DLQ, tardanza, tamaño de estado, checkpoint duration/failures, backpressure, p50/p95 de serving, cobertura/demora de labels, Brier/costo por período y distribución de features. Diferenciar problemas del modelo de los de ingestión.

## Seguridad y operación cotidiana

El broker sandbox escucha sólo en loopback y usa PLAINTEXT de laboratorio. Un despliegue compartido necesita TLS/SASL, permisos por servicio y políticas de retención/PII. El navegador no debe tener acceso directo al broker. Los nombres de campañas de este repo son sintéticos.

El normalizador elimina campos no permitidos y la DLQ educativa conserva sólo motivos. La captura de consentimiento del fixture no reemplaza el mecanismo del producto ni configura GTM. Un ejecutor real debe volver a validar permisos antes de activar y contemplar revocación, supresión y límites de frecuencia.

Para unir una transacción de negocio con su evento de forma confiable, estudiar transactional outbox/CDC: un commit de base de datos y un publish independiente pueden desincronizarse. Reintentos, dedup y reconciliación deben diseñarse juntos.

## Costos y limpieza

Los demos locales no crean recursos cloud. La descarga de Flink/JDK/Kafka ocupa disco local; `.runtime/` y `artifacts/` están ignorados por Git. Los scripts no reformatean el almacenamiento de Kafka que ya tenías.

En Confluent, verificar cluster, conectores, statements/pools y almacenamiento. En Tableflow, sumar S3, catálogo y Athena. Revisar un plan de destrucción contra el estado correcto; no correr teardown de un taller sobre infraestructura compartida. Documentar costo estimado y costo observado como campos separados.
