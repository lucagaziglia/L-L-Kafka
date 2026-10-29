# Cómo funciona el análisis y el ML en tiempo real

## Qué hace cada componente

| Componente | Responsabilidad | Ejemplo |
|---|---|---|
| Productor | Emitir el hecho y su identidad | Operación observada con monto, riesgo de dispositivo e intentos recientes |
| Kafka | Conservar, distribuir y permitir replay | Topic `risk.observed.v1`, key `account_id` |
| Flink | Validar y derivar información del stream | Contar intentos en una ventana, unir contexto conocido al momento |
| Scoring | Aplicar una versión del modelo a features | `predict_proba(x)` |
| Política | Traducir score a decisión bajo costos/restricciones | Enviar a revisión si p≥0,25 |
| Ejecutor | Aplicar la acción con idempotencia | Registrar una revisión por `decision_id` |
| Feedback | Traer la etiqueta cuando se conoce | Resultado de investigación, conversión o devolución |
| Evaluación | Comparar predicción histórica con resultado | Brier, precision, recall y costo por período |

El ejemplo de riesgo es un ejercicio estadístico sintético, sin conexión a operaciones financieras reales.

## Matemática del modelo incluido

Para un evento construimos `x = [1, amount/500, device_risk, attempts_1m/10]`. El primer elemento es el intercepto. Los divisores son constantes fijas: no se estiman mirando el futuro. Los valores se validan y se recortan para evitar magnitudes extremas en el ejercicio.

Matemáticamente, `x ∈ R^(4×1)` y `w ∈ R^(4×1)`. El producto compatible `wᵀx` tiene forma `(1×4)(4×1) = (1×1)` y produce el logit `z`. La probabilidad es `p = σ(z) = 1/(1 + exp(-z))`.

Para n observaciones, `X ∈ R^(n×4)`, `w ∈ R^(4×1)`, `Xw ∈ R^(n×1)`, `y ∈ {0,1}^(n×1)`. En el código no hay matrices NumPy: `weights` y `features()` son listas de longitud 4 y el producto es una suma de productos elemento a elemento.

La pérdida por observación es `L = -y log(p) - (1-y) log(1-p)`. El gradiente respecto de `w` es `(p-y)x`, de forma `(4×1)`. La actualización implementada es descenso de gradiente estocástico con una pequeña penalización L2 en los pesos, excluyendo el intercepto:

`w ← w - η[(p-y)x + λw_penalizado]`.

La logística es útil aquí porque podés inspeccionar toda la matemática. Después compará árboles/boosting y un learner incremental como River; cambiá una cosa por vez y mantené el mismo protocolo temporal.

## Entrenar no es servir

`demo-ml` entrena con las primeras 1.000 filas históricas, observa 200 de validación y evalúa 400 de test posteriores. Las etiquetas históricas existen al entrenar. En el stream, las observaciones **no incluyen la etiqueta**; ésta llega por separado ocho observaciones después. El modelo congelado sirve predicciones sin cambiar sus pesos.

Con `--online-learning`, la predicción se guarda primero y el modelo aprende sólo cuando llega su etiqueta. La métrica usa la probabilidad que se emitió originalmente. Esto se llama evaluación prequential o test-then-train con feedback demorado. No implica que todo modelo deba entrenarse por evento.

Los nombres `normal` y `drift` son anotaciones del generador para evaluar el ejercicio: **no son features ni una señal disponible para un detector de drift real**.

## De probabilidad a decisión

Supongamos costo de falso positivo `C_FP=30` y falso negativo `C_FN=90`, con costo cero en aciertos. Actuar cuesta `(1-p)·30`; no actuar cuesta `p·90`. Se elige actuar cuando `p > 30/(30+90) = 0,25`; en igualdad, la implementación elige revisar.

Son unidades de costo didácticas, no dinero ni costos reales. El umbral se fija por esos supuestos y no se optimiza sobre el test. Si una revisión tiene costo propio, capacidad limitada o riesgos diferentes, la política cambia. Una probabilidad mal calibrada también degrada esta decisión.

## Qué mirar en los reportes

**Brier:** promedio de `(p-y)²`; menor es mejor para calidad probabilística. **Log loss:** penaliza predicciones seguras equivocadas. **Precision/recall:** muestran la mezcla de revisiones falsas y positivos perdidos. **Costo medio:** conecta el umbral con los supuestos de negocio. El Brier contra una constante es un baseline simple; no reemplaza curvas de calibración, AUC/PR ni intervalos de incertidumbre.

El generador cambia deliberadamente la relación entre `device_risk` y la etiqueta a mitad del stream. Mirar los tramos por separado permite ver degradación que un promedio global oculta. En producción hay que medir drift con métodos y ventanas explícitos y distinguirlo de problemas de ingestión, cambios de mezcla o retrasos en labels.

## Features y tiempo: dónde aparece el leakage

Si puntúo a las 10:01, no puedo usar la compra de las 10:03 ni una versión de CRM cargada a las 10:05 como si ya la conociera. Un join temporal y un dataset point-in-time ayudan a conservar esa restricción. `attempts_1m` del demo es una feature sintética ya calculada y disponible antes de decidir; no se presenta como una medición real ni como una salida calculada por Flink.

El SQL adicional de ventanas calcula agregados a su cierre. Si usás ese agregado para predecir cada evento anterior de la ventana, podés filtrar futuro. Usalo para una decisión **al cierre de la ventana**, o implementá una ventana hacia atrás para cada evento y reproducí exactamente esa definición al entrenar.

## Evolución del laboratorio a un servicio

Separar repositorio histórico/entrenamiento de serving. Registrar esquema y orden de features, versión de modelo, fecha de entrenamiento, política/umbral y límites. Comparar champion/challenger con datos posteriores; desplegar en shadow antes de habilitar acciones. Medir latencia de recepción→decisión con un reloj de proceso y reloj de sistema bien definidos. Guardar predicciones para evaluarlas cuando llegue el resultado.

Los demos tienen estado en memoria y un stream finito. Para un servicio continuo hacen falta expiración de pending labels, dedup persistente, restauración y coordinación entre offsets y salidas. Flink ofrece checkpoints para su propio estado; eso no vuelve transaccional un servicio Python externo ni el envío de un email.
