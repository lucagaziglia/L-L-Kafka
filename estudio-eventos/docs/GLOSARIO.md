# Glosario para la primera vuelta

| Término | En este recorrido |
|---|---|
| Event-driven architecture | Componentes reaccionan a hechos publicados con contratos claros |
| Event streaming | Procesamiento continuo sobre secuencias de eventos |
| Event sourcing | Usar un historial de eventos como fuente de verdad para reconstruir estado; no todo uso de Kafka es event sourcing |
| CQRS | Separar modelos de escritura y lectura; puede usarse con eventos, sin ser sinónimo |
| Producer / consumer | Quien publica / quien lee y procesa |
| Topic / partition | Flujo lógico / unidad de log y orden |
| Key / offset | Identidad para particionar / posición dentro de una partición |
| Consumer group / lag | Consumidores que reparten particiones / distancia respecto del avance del log |
| KRaft | Gestión de metadatos de Kafka basada en Raft |
| Event time / processing time | Instante del hecho / instante en que el sistema lo procesa |
| Watermark | Estimación de avance de event time usada para completar cómputos |
| TUMBLE / HOP / SESSION | Ventanas fijas sin solapamiento / solapadas / agrupadas por actividad |
| Keyed state / timer | Estado por clave / disparador temporal asociado al procesamiento |
| Checkpoint / savepoint | Snapshot automático para recuperación / snapshot controlado para operaciones y evolución |
| Backpressure | Un tramo lento limita el avance de los anteriores |
| Dynamic table / changelog | Tabla que se mantiene con el flujo / secuencia de inserciones, cambios y bajas |
| Interval join / temporal join | Correlación dentro de un horizonte / contexto según versión temporal |
| Idempotencia / dedup | Repetir no cambia el efecto / reconocer repeticiones según una identidad y retención |
| DLQ | Destino de errores para inspección o reparación, con política de datos |
| At-least-once / exactly-once | Posibles redeliveries / efecto único dentro del alcance y configuración que lo garantizan |
| Schema Registry | Registro y políticas de contratos serializados |
| Outbox / CDC | Patrón para publicar a partir de cambios confirmados / captura de cambios de una base |
| Feature / label | Información disponible al predecir / resultado que se quiere aprender o evaluar |
| Inference / online learning | Aplicar un modelo / actualizarlo incrementalmente con feedback |
| Prequential evaluation | Conservar la predicción previa al aprendizaje y evaluarla al conocer la etiqueta |
| Drift / train-serve skew | Cambio de distribución o relación X→y / diferencia entre features de entrenamiento y serving |
| Propensión / uplift | Probabilidad de un resultado / efecto diferencial de una intervención bajo un diseño adecuado |
| SLO / SLA | Objetivo medible del servicio / compromiso acordado del servicio |
