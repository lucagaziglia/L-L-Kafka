# L&L · De eventos de marketing a sistemas que deciden

Un recorrido práctico para Luca y Lucas: **arquitectura orientada a eventos → Kafka → Flink → analítica y ML en tiempo real → marketing**. El punto de partida es la experiencia con Analytics/GTM y Python/SQL; el destino es poder diseñar, programar, explicar y operar un sistema pequeño con decisiones reproducibles.

## Empezá acá

1. Leé el [plan de 16 semanas](PLAN_DE_ESTUDIO.md): 8 horas por semana, 128 horas estimadas. Se puede estirar a 32 semanas con 4 h/semana.
2. Leé el [puente desde WILDFI, GA4 y GTM](docs/01_PUENTE_DESDE_ANALYTICS.md).
3. Ejecutá el [primer encuentro de 90 minutos](labs/00_PRIMER_ENCUENTRO.md).
4. Hacé la [práctica de claves, particiones y grupos](labs/00B_KAFKA_KEYS_Y_GRUPOS.md).
5. Seguí los cuatro hands on de la tabla, en orden.

| Hands on | Qué construís | Tiempo activo estimado | Modalidad |
|---|---|---:|---|
| [01 · Kafka/Flink y calidad](labs/01_CONFLUENT_FUNDAMENTOS.md) | Consultas continuas, deduplicación, ventanas | 3–4 h | Confluent Cloud, basado en workshop oficial |
| [02 · Productos de datos](labs/02_CONFLUENT_PRODUCTOS.md) | Join de órdenes/pagos y enriquecimiento temporal | 3–4 h | Confluent Cloud, basado en workshop oficial |
| [03 · Analítica + ML](labs/03_ANALITICA_ML.md) | Modelo entrenado, scoring, decisiones y etiquetas demoradas | 6–8 h | Python local + Kafka real; extensión Flink SQL |
| [04 · Marketing](labs/04_MARKETING_GA4_GTM.md) | Normalización, métricas, consentimiento y activaciones compensables | 6–8 h | Python local + Kafka real + SQL para Flink |

Estos tiempos son sesiones dentro del plan, no horas adicionales a las 128.

## Primera ejecución, sin cuentas ni dependencias de Python

Desde **esta carpeta** (`estudio-eventos`), con Python 3.10 o posterior:

```bash
python3 -m ll_events demo-all
python3 -m unittest discover -s tests -v
```

Se generan `artifacts/model.json`, `artifacts/ml-report.json`, `artifacts/decisions.jsonl`, `artifacts/marketing-report.json` y eventos JSONL. Todos los datos son sintéticos, con semillas fijas. `artifacts/` no se sube a Git.

Los dos demos anteriores son procesadores educativos en Python. Para usar **Kafka real**, aprovechando la distribución y el JDK que ya están en la carpeta padre:

```bash
python3 scripts/kafka_demo.py --case ml
python3 scripts/kafka_demo.py --case marketing
```

Cada ejecución crea su propio broker en `127.0.0.1:19092`, con controller en `19093`, topics con identificador de corrida y datos aislados en `artifacts/`. Comprueba que las decisiones emitidas se puedan volver a leer de Kafka y detiene los procesos que inició. Si los puertos están ocupados, se detiene sin afectar el proceso existente. En otra máquina, configurar `KAFKA_HOME` y `JAVA_HOME`. No ejecutar ambos casos simultáneamente con los mismos puertos.

Para **Flink real**: [guía local](docs/04_FLINK_LOCAL.md). Los SQL locales están en `sql/local/`; los SQL de Confluent están en `sql/cloud/`. Los catálogos, conectores y opciones difieren entre ambas modalidades.

## Resultados esperados

En el demo ML, 240 observaciones generan 240 decisiones; 15 duplicados se ignoran y todas las etiquetas llegan más tarde. En el holdout sintético, el Brier del modelo ronda **0,194** frente a **0,250** de una probabilidad constante. Después de un cambio deliberado del proceso generador, el Brier empeora: es la señal para investigar drift, no una garantía automática de reentrenamiento.

En marketing se esperan **16 entradas**, **11 aceptadas**, un duplicado de evento, un duplicado de transacción, un evento sin consentimiento, uno inválido y una compra tardía. Se emiten dos elegibilidades de asistencia y una cancelación. El ingreso en las ventanas es **USD 120**; la compra tardía de USD 75 queda para reconciliación, sin reabrir esas métricas.

Ver [evidencia y alcance de la validación](evidencias/VALIDACION.md) para distinguir lo ejecutado de los ejercicios que requieren tu cuenta cloud.

## Mapa del material

- [Plan y entregables semanales](PLAN_DE_ESTUDIO.md).
- [Arquitectura y matemática del ML](docs/02_ARQUITECTURA_Y_ML.md).
- [Contratos, fallos y operación](docs/03_CONTRATOS_Y_OPERACION.md).
- [Fuentes oficiales y revisión fijada del workshop](docs/FUENTES.md).
- [Glosario](docs/GLOSARIO.md) y [bitácora editable](BITACORA.md).
- `ll_events/`: lógica ejecutable, legible y sin frameworks.
- `tests/`: verificaciones de comportamiento, duplicados, fuga de información y consentimiento.
- `examples/`: ejemplos de dataLayer y contrato JSON Schema.

Este laboratorio no envía comunicaciones ni modifica una propiedad GA4, un contenedor GTM o campañas publicitarias. Las acciones son registros para inspeccionar. Los ejemplos de marketing y el detector sintético de riesgo son proyectos de aprendizaje separados del trading y del MMM.
