# Primer encuentro · 90 minutos

Objetivo: ver un evento convertirse en decisión y poder explicar qué parte corresponde a datos, modelo y transporte. Trabajá desde `estudio-eventos/`.

## 0–15 min · Dibujar antes de programar

Leé el puente desde Analytics. Dibujá una compra con tres fuentes: navegador, órdenes y pagos. Marcá cuál puede duplicarse, cuál confirma ingreso y qué ID permite correlacionarlas. Explicá por qué un trigger de GTM y un consumidor Kafka resuelven problemas diferentes.

## 15–35 min · Ejecutar ML sin infraestructura

```bash
python3 -m ll_events demo-ml
```

Abrí `artifacts/risk-events.jsonl`, `artifacts/decisions.jsonl`, `artifacts/model.json` y `artifacts/ml-report.json`. Buscá `live-00000` y su `label-00000`: primero existe una probabilidad y después una etiqueta. La observación no lleva la etiqueta. Compará el Brier de test con `constant_baseline_test`.

Pregunta para explicar: ¿por qué se puede servir el modelo congelado sin reentrenarlo? ¿Qué cambia realmente con `--online-learning`?

## 35–55 min · Ejecutar marketing

```bash
python3 -m ll_events demo-marketing
```

Abrí `artifacts/marketing-report.json`. Encontrá el duplicado, la transacción repetida, el evento descartado por consentimiento y la compra tardía. Explicá por qué hay una cancelación y por qué la facturación de ventanas sigue siendo 120.

## 55–75 min · Agregar transporte real

Si tenés el Kafka/JDK de la carpeta padre:

```bash
python3 scripts/kafka_demo.py --case ml
```

El programa imprime el topic de entrada, el de decisiones y el archivo de evidencia. Verificá `verified: true` e igualdad entre `decision_count` y `persisted_count`. No es una prueba de alta disponibilidad ni de exactly-once: sí comprueba un recorrido real productor→broker→consumidor→broker.

Si estás en otra máquina y todavía no instalaste Kafka, completá primero la guía de prerequisitos del repositorio padre y configurá `KAFKA_HOME`/`JAVA_HOME`. El demo Python de los pasos anteriores funciona igual.

## 75–90 min · Review

```bash
python3 -m unittest discover -s tests -v
```

Registrá una sesión en la bitácora. Entregable: diagrama de compra, reporte ML, reporte marketing y una explicación de dos minutos de la diferencia entre evento, feature, predicción y decisión. Próxima sesión: partitions, keys y consumer groups.
