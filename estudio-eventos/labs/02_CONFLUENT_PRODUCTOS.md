# Hands on 02 · De streams a productos de datos

**Duración:** 3–4 horas. **Base:** Lab 2 del workshop Confluent fijado en [FUENTES](../docs/FUENTES.md). Prerrequisito: hands on 01 y tabla `ll_unique_orders_v1` en el catálogo/base del workshop.

La pregunta es concreta: ¿qué órdenes tienen un pago relacionado dentro de diez minutos y con qué contexto del cliente debemos analizarlas?

## 1 · Join que necesita límites (45 min)

Abrí [02_productos.sql](../sql/cloud/02_productos.sql). Describí `payments` y verificá `order_id`, `payment_time` y `amount`. El timestamp tiene que ser un atributo temporal válido para el plan elegido; inspeccioná el schema generado por la revisión del workshop.

Ejecutá la consulta de interval join: la orden debe estar entre diez minutos antes del pago y el momento del pago. Un join sin horizonte puede acumular estado indefinidamente. Explicá qué pagos excluye la regla: pagos anteriores a la orden y pagos más de diez minutos después.

Luego ejecutá `CREATE TABLE ll_valid_orders_v1 ... AS SELECT ...`, que conserva un `order_time` con watermark. Crearlo inicia trabajo continuo; registrá el statement para detenerlo después.

Resultado esperado: órdenes correlacionadas con pagos bajo esa regla. Un pago coincidente no demuestra por sí solo que una compra sea única si upstream permite varios pagos por orden: escribí una política de pago parcial, reintentos y reversas antes de usarlo como ingreso certificado.

## 2 · Contexto correcto en el tiempo (60 min)

Primero ejecutá `DESCRIBE customers`, `SHOW CREATE TABLE customers` y las equivalentes de `products`. Un temporal join necesita una dimensión versionada adecuada, clave primaria y atributos temporales compatibles. `PRIMARY KEY NOT ENFORCED` no limpia duplicados por vos: el productor debe cumplir el contrato.

La consulta con `FOR SYSTEM_TIME AS OF o.order_time` pide el contexto disponible según la semántica temporal de esa tabla. Comparala conceptualmente con un join al CRM actual: si el segmento cambió, ¿qué responde cada uno? No reemplaces el temporal join por un join regular si falla el prerrequisito sin documentar que cambió la pregunta.

Resultado esperado: stream enriquecido con marca/producto y campos del cliente. Para el ejercicio se proyecta `name`, sin propagar email al producto de salida.

## 3 · Una política de loyalty, no un modelo causal (45 min)

Agregá el gasto observado por cliente en `ll_valid_orders_v1`. La consulta clasifica `gold` desde 500 y `standard` por debajo. Es una regla de negocio y una agregación que puede actualizarse; no es entrenamiento de ML ni prueba de incrementalidad.

Como el gasto es acumulado, elegí cómo limitar/retener estado y qué significa expirar clientes. Para materializar una salida que cambia, elegí un sink compatible con upserts/changelog; no publiques actualizaciones como compras nuevas en un topic append-only de hechos.

## 4 · Extensión de negocio y review (45 min)

Diseñá un producto de datos con owner, contrato, key, SLA/SLO, política de tardanza, retención y consumidor. Ejemplo: `customer.loyalty.updated.v1`, key `customer_id`. Explicá la diferencia entre ese cambio de estado y `CustomerPurchased`, que es un hecho del dominio.

Entregables: SQL y plan del join, una tabla de las hipótesis temporales, contrato del producto y política de corrección. El revisor debe poder describir qué ocurre si el pago llega tarde o el CRM se actualiza.

## Extensiones posteriores

El workshop trae Tableflow/Iceberg/Athena y ejemplos de Table API, UDFs y Terraform. Hacerlos después de dominar el núcleo. Los labs Tableflow requieren AWS y recursos adicionales; no son prerequisitos para el ejercicio ML o marketing local.

Al terminar, detener los statements y seguir la limpieza del hands on 01.
