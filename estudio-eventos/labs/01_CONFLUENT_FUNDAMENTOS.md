# Hands on 01 · Flink SQL, calidad y ventanas

**Duración:** 3–4 horas con pausas para explicar. **Base:** Lab 1 del [workshop Confluent](https://github.com/confluentinc/confluent-cloud-flink-workshop/tree/14be8478f42dfd1ddef144011b6611b95f5b1687). Esta guía agrega preguntas y criterios de verificación propios.

## Qué necesitás

SQL básico y un entorno Confluent Cloud del workshop. Si asistís a un taller, usá sólo el acceso provisto por el instructor. Si lo hacés por tu cuenta, seguí el README de esa revisión y `demo-infrastructure/`: cuenta, API keys con el alcance requerido por el provisioning, Terraform y Git. Se crean recursos facturables; revisá el plan antes de aplicarlo y eliminá los recursos del taller al terminar.

No uses credenciales en Markdown ni las subas a Git. Los archivos `.tfvars`, estados de Terraform y `env.sh` generado deben quedar fuera del repositorio. Este material no provisiona tu cuenta automáticamente.

## 1 · Entender el entorno (30 min)

En el SQL Workspace seleccioná el catálogo del environment y la base del cluster del workshop. Comprobá los topics `orders`, `payments`, `products`, `customers`, `clicks`, `order_status` y `customer_inquiries`, sus esquemas y los pools para generación y consultas.

Ejecutá **una sentencia por celda** de [01_fundamentos.sql](../sql/cloud/01_fundamentos.sql). Primero `SHOW TABLES`, luego `DESCRIBE orders` y `DESCRIBE EXTENDED orders`. Dibujá el mapeo Environment→Catalog, Cluster→Database, Topic+Schema→Table que utiliza Confluent Cloud.

Resultado esperado: eventos nuevos en los SELECT continuos. A diferencia de una consulta batch, un SELECT puede seguir ejecutándose. Detené consultas exploratorias al terminar.

## 2 · Ver el problema de los duplicados (45 min)

Contá ocurrencias por `order_id`. El generador no debe interpretarse como datos reales de comercio. Escribí qué clave de negocio estás protegiendo y qué querés conservar: primer registro, último estado o versión vigente.

La guía crea `ll_unique_orders_v1` con `ROW_NUMBER` por orden, usando el `$rowtime` que expone Confluent Cloud. Antes de crear la tabla, corré `EXPLAIN` y guardá el plan. `sql.state-ttl = 1 hour` limita la retención del estado de dedup en esta práctica: un duplicado mucho más tarde puede volver a considerarse nuevo. No es una garantía de unicidad perpetua.

Resultado esperado: un registro elegido por orden dentro de la semántica y retención configuradas. Inspeccioná `SHOW CREATE TABLE ll_unique_orders_v1` y el changelog del plan. No deduzcas garantías de inserción única sólo por el nombre de la tabla; dependen del plan, watermark, tardanza y sink.

## 3 · Event time y ventanas (45 min)

Ejecutá la ventana de un minuto y después la HOP de diez minutos que avanza cada cinco. Contá la misma orden en ambas. La HOP puede incluir un evento en más de una ventana: sumar sus resultados como si fueran períodos disjuntos duplica contribuciones.

Preguntas: ¿cuál es el timestamp que se usa?, ¿cuándo avanza el watermark?, ¿por qué puede no aparecer una ventana si el stream deja de avanzar?, ¿qué demora aceptable cambia la decisión de negocio?

## 4 · Resolver y romper (45 min)

1. Cambiá la ventana de 1 a 2 minutos y explicá el cambio antes de ejecutarlo.
2. Compará volumen bruto y deduplicado en el mismo intervalo.
3. Inspeccioná el estado/lag del statement que mantiene la tabla, y diferenciá el de generación.
4. Escribí qué pasaría con un duplicate después de expirar el TTL. Es una hipótesis a verificar con un fixture controlado, no un resultado demostrado por mirar el stream aleatorio.

## Entregable y criterio de aprobación

Guardá SQL, `SHOW CREATE TABLE`, `EXPLAIN`, nombres de statements y observaciones en la bitácora. Aprobás cuando podés explicar key, estado, TTL y watermark, y mostrar una comparación de entrada/salida. Para un input fijo con resultados exactos y sin cuenta cloud, usar el fixture de la guía de [Flink local](../docs/04_FLINK_LOCAL.md).

## Limpieza

Detené primero los statements continuos creados por vos; después eliminá sus tablas de laboratorio si ya no se necesitan. Si provisionaste el entorno del workshop, revisá `terraform plan -destroy` y luego `terraform destroy` **desde su directorio y estado**. Detener SELECT no elimina automáticamente cluster, conectores, compute ni almacenamiento.
