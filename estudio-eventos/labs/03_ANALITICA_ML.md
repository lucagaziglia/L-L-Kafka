# Hands on 03 · Analítica y decisiones de ML en tiempo real

**Duración:** 6–8 horas en dos sesiones. **Caso:** operaciones sintéticas con distintos perfiles de riesgo. **Objetivo:** comprender un modelo por dentro, servirlo sobre eventos y medirlo con feedback demorado.

## 1 · Datos y pregunta (45 min)

Leé [arquitectura y matemática](../docs/02_ARQUITECTURA_Y_ML.md). Inspeccioná `ll_events/generate.py`: las etiquetas se generan probabilísticamente, no son una regla determinista que copiamos al clasificador. `amount`, `device_risk` y `attempts_1m` ya existen antes de predecir. `attempts_1m` es sintético; no afirmar que Flink lo calculó.

La decisión es `review` o `allow` en una simulación. La etiqueta posterior indica si la operación era positiva. Ninguna acción se aplica sobre operaciones reales.

## 2 · Baseline y entrenamiento (60 min)

```bash
python3 -m ll_events demo-ml --out artifacts/ml-frozen
```

Abrí `model.json` y `ml-report.json`. Dibujá las formas de X, w e y. Hacé a mano una predicción con cuatro números y contrastala con `Logistic.predict`. Identificá los cortes temporales 1000/200/400 y explicá por qué no mezclarías aleatoriamente un dataset con cambios de régimen.

Esperado en test, con la semilla por defecto: Brier ≈0,1936 frente a ≈0,2502 del baseline constante; costo medio 13,275 frente a 15. Son resultados sintéticos del ejercicio, no una validación de fraude real. El threshold se fija por costos antes de evaluar test.

## 3 · Servir primero, conocer la verdad después (60 min)

Inspeccioná los JSONL: `risk.observed.v1` no lleva label y `risk.label_arrived.v1` referencia `observation_id`. El motor guarda x y la probabilidad original hasta recibir el resultado. Repetir la observación no debe generar otra decisión en la misma ejecución; el ID de decisión es determinista para modelo congelado.

```bash
python3 -m unittest discover -s tests -v
python3 scripts/kafka_demo.py --case ml
```

Con Kafka se esperan 165 mensajes para 80 observaciones únicas, 5 redeliveries y 80 labels. Se verifican 80 decisiones persistidas. El broker del ejercicio tiene una partición para reproducir el orden total del fixture. Después repetí el razonamiento con tres particiones y key de entidad: Kafka conserva orden por partición; el orden entre entidades no está garantizado.

## 4 · Comparar con aprendizaje incremental (60 min)

```bash
python3 -m ll_events demo-ml --online-learning --out artifacts/ml-online
```

Compará los reportes por tramo `normal`/`drift`. No elijas sólo el promedio que se ve mejor. Registrá precisión, recall, Brier, costo y cantidad de labels. El modo online modifica el objeto de modelo sólo cuando llega la etiqueta; el `model.json` conserva el modelo inicial entrenado para poder repetir el arranque.

Propuesta de experimento: cambiá la demora de labels de 8 a 40 observaciones, luego cambiá la tasa de aprendizaje. Anotá una hipótesis antes de correr. Evaluá sobre una nueva semilla sin usarla repetidamente para elegir parámetros. Tener feedback rápido no garantiza adaptación correcta.

## 5 · Incorporar Flink con una pregunta temporal (90 min)

Seguí la guía [Flink local](../docs/04_FLINK_LOCAL.md). `sql/local/02_risk_windows.sql` calcula conteo y monto por entidad/minuto desde Kafka con event time; `01_bounded_windows.sql` usa un fixture fijo sin Kafka para verificar el resultado.

Son agregados para una decisión al **cierre** de la ventana, separados del scoring por observación del demo. Si querés usarlos para el modelo, generá un histórico con esa misma definición, entrená una nueva versión y usá `window_end` como instante de decisión. No intercambies contratos silenciosamente.

Desafío: añadir un consumidor de métricas cerradas, entrenar un modelo por entidad/ventana y emitir decisiones con `feature_window_end`. Alternativa más avanzada: implementar rolling features causales por evento con keyed state/timers. Medí skew entre entrenamiento y serving.

## 6 · Diseñar fallos (45 min)

- Duplicar una observación y una label: una decisión y un aprendizaje como máximo por ID dentro del motor.
- Entregar label antes de la observación: falla explícita; diseñar buffering con TTL si se usan topics separados.
- Reiniciar el motor: su dedup se pierde; diseñar persistencia y coordinación con offsets antes de afirmar exactly-once.
- Perder labels: definir expiración, cobertura de feedback y sesgo de selección.

## Entregable

Un informe con baseline, split temporal, versión de features/modelo, umbral y costos supuestos, diagrama de componentes, outputs Kafka, comparación congelado/online y plan de recuperación. Incluí una decisión que cambió por el modelo y otra limitación que éste no resuelve.
