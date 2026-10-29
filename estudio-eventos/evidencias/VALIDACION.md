# Validación de entrega · 29/10/2026

## Ejecutado y verificado

| Comprobación | Resultado | Evidencia |
|---|---|---|
| Pruebas de comportamiento Python | 16/16 pasaron | `python3 -m unittest discover -s tests -v` |
| Modelo con split temporal | 1000 train / 200 validation / 400 test; Brier test 0,1936 vs constante 0,2502 | [ml-frozen.json](ml-frozen.json) |
| Scoring congelado y labels demoradas | 240 decisiones únicas, 15 duplicados ignorados, 0 labels pendientes | [ml-frozen.json](ml-frozen.json) |
| Aprendizaje incremental | Corre y conserva predicción previa a recibir cada etiqueta | [ml-online.json](ml-online.json) |
| Marketing local | 16 entradas; 11 aceptadas; 2 elegibilidades y 1 cancelación; USD 120 en ventanas | [marketing.json](marketing.json) |
| Kafka → Python ML → Kafka | 165 entradas; 80 decisiones emitidas y 80 leídas idénticas del topic | [kafka-ml.json](kafka-ml.json) |
| Kafka → Python marketing → Kafka | 16 entradas; 3 acciones emitidas y 3 leídas idénticas del topic | [kafka-marketing.json](kafka-marketing.json) |
| Flink real, ventana streaming con fuente finita | Tres inputs; exactamente `(a,2,30)` y `(b,1,50)` | [flink-bounded.json](flink-bounded.json) |
| Kafka → Flink real, snapshot acotado | 80 observaciones / monto 26991,8; marketing dedup: 12 eventos / USD 195 | [flink-kafka.json](flink-kafka.json) |
| Sintaxis de archivos y enlaces Markdown locales | Python/JSON parseables y enlaces internos resueltos | Revisión de archivos de entrega |

Entorno utilizado: macOS, Python 3.14, Kafka 4.3.1 y JDK Temurin 21.0.12.1 ya existentes en la carpeta L&L-Kafka. Se descargó Apache Flink 2.1.3 y Kafka SQL connector 5.0.0-2.1 desde sus fuentes oficiales y se verificaron sus checksums. Java 21 tiene soporte experimental en esta rama de Flink; la documentación recomienda Java 17.

Los brokers de las pruebas son temporales, con puertos 19092/19093 y almacenamiento propio. No se reformateó ni se inició el almacenamiento habitual de Kafka de la carpeta padre. Los procesos iniciados por las pruebas se detuvieron al terminar.

## Cómo interpretar la diferencia 120 vs 195 en marketing

El simulador Python procesa en orden de llegada y avanza un watermark global. Una compra de USD 75 llega después de ese watermark: cancela una asistencia pero no reabre una ventana cerrada, que queda en USD 120. El ensayo Kafka/Flink usa **batch sobre un snapshot bounded** y reconcilia todos los eventos válidos deduplicados, incluida esa compra: USD 195. Se verifican dos preguntas temporales diferentes; no es una discrepancia ocultada ni una prueba de tardanza streaming en Flink.

## Preparado para ejecutar, sin ejecución cloud en esta entrega

- Hands on 01/02 y SQL de Confluent: revisados contra la revisión fijada del workshop; requieren recursos y acceso a la cuenta cloud del alumno.
- Prueba de fallos con checkpoint/restore y backpressure: definida como ejercicio; no se declara alta disponibilidad ni recuperación transaccional verificada.
- GTM/GA4: snippet y pasos preparados; no se instaló un contenedor, una propiedad, un collector ni se enviaron hits.
- Serving continuo de ML con features calculadas por Flink: arquitectura y extensiones documentadas. Las pruebas ejecutadas separan Kafka→Python scoring de Kafka→Flink agregados; no se presenta un único circuito Flink→scoring desplegado.
- Experimento causal/uplift: diseño de actividad; no se fabricaron resultados incrementales.
- Carga/latencia/SLO y costos cloud: no medidos; las metas están identificadas como propuestas.

Todos los datos y resultados estadísticos de los ejemplos son sintéticos. El material es un laboratorio de aprendizaje, no una certificación de producción.
