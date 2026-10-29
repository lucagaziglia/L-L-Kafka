# Fuentes y trazabilidad

Material preparado y referencias consultadas el **29/10/2026**. Las interfaces cloud pueden cambiar; el workshop se fija por commit para conservar trazabilidad.

## Workshop que estructura los dos primeros hands on

- Repositorio: [confluentinc/confluent-cloud-flink-workshop](https://github.com/confluentinc/confluent-cloud-flink-workshop).
- Revisión utilizada: [`14be8478f42dfd1ddef144011b6611b95f5b1687`](https://github.com/confluentinc/confluent-cloud-flink-workshop/tree/14be8478f42dfd1ddef144011b6611b95f5b1687).
- [README y provisioning](https://github.com/confluentinc/confluent-cloud-flink-workshop/blob/14be8478f42dfd1ddef144011b6611b95f5b1687/README.md).
- [Lab 1](https://github.com/confluentinc/confluent-cloud-flink-workshop/blob/14be8478f42dfd1ddef144011b6611b95f5b1687/flink-getting-started/lab1.md): fundamentos, dedup, EXPLAIN, agregación y ventanas.
- [Lab 2](https://github.com/confluentinc/confluent-cloud-flink-workshop/blob/14be8478f42dfd1ddef144011b6611b95f5b1687/flink-getting-started/lab2.md): correlación de órdenes/pagos, enriquecimiento y productos de datos.

Los hands on 01/02 y sus SQL son adaptaciones educativas; los ejercicios Python ML/marketing y el fixture local son desarrollos propios para este recorrido. El upstream no se presenta como si incluyera estos algoritmos. Ver [THIRD_PARTY_NOTICES](../THIRD_PARTY_NOTICES.md).

Para obtener esa revisión en tu ambiente de taller:

```bash
git clone https://github.com/confluentinc/confluent-cloud-flink-workshop.git
cd confluent-cloud-flink-workshop
git checkout 14be8478f42dfd1ddef144011b6611b95f5b1687
```

## Semanas 1–4 · Kafka y eventos

- [Introducción oficial a Kafka](https://kafka.apache.org/intro/).
- [Quickstart oficial](https://kafka.apache.org/quickstart/): usar comandos de la versión instalada.
- [Documentación Kafka](https://kafka.apache.org/documentation/): diseño, productores, consumidores y configuración.
- [Confluent Developer: patrones de event streaming](https://developer.confluent.io/patterns/): para profundizar diseño, claves, procesamiento y entrega.

## Semanas 5–8 · Flink

- [Descargas y matriz de conectores](https://flink.apache.org/downloads/): conectores se publican separadamente.
- [Kafka SQL connector, rama 2.1](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/connectors/table/kafka/).
- [Compatibilidad Java, rama 2.1](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/deployment/java_compatibility/).
- [Watermarks](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/datastream/event-time/generating_watermarks/).
- [Estado y checkpoints](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/datastream/fault-tolerance/checkpointing/).
- [Ventanas en Confluent Cloud](https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html).
- [Dynamic tables en Confluent Cloud](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html).

## Semanas 9–11 · ML continuo

- [scikit-learn: aprendizaje incremental y escalabilidad](https://scikit-learn.org/stable/computing/scaling_strategies.html).
- [River: documentación oficial](https://riverml.xyz/latest/): extensión posterior para learners incrementales y evaluación progresiva.

Estas lecturas amplían los conceptos; el código de la primera vuelta usa sólo Python estándar para que puedas inspeccionar las operaciones.

## Semanas 12–16 · Medición y marketing

- [Google: eventos GA4](https://developers.google.com/analytics/devguides/collection/ga4/events).
- [Google: ecommerce GA4](https://developers.google.com/analytics/devguides/collection/ga4/ecommerce).
- [Google: dataLayer](https://developers.google.com/tag-platform/tag-manager/datalayer).
- [Google: Measurement Protocol](https://developers.google.com/analytics/devguides/collection/protocol/ga4) y [referencia](https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference).
- [Google: validación de eventos Measurement Protocol](https://developers.google.com/analytics/devguides/collection/protocol/ga4/validating-events).

El enfoque de causalidad del plan es un criterio metodológico: la existencia de Kafka, una correlación o una predicción no identifica el efecto de una intervención. El diseño experimental se deja como entregable explícito, sin presentar resultados causales fabricados.
