# Flink real en tu computadora

## Versiones y prerequisitos

Este recorrido fija **Apache Flink 2.1.3** y **Kafka SQL connector 5.0.0-2.1**, una combinación publicada para esa rama. No pretende usar automáticamente la versión más nueva: las releases de conectores son independientes. Java **17 es el recomendado** por la documentación de la rama; Java 21 tiene soporte experimental. La verificación de entrega registra el JDK efectivamente usado.

Necesitás Python 3.10+, `curl`, espacio libre para la descarga/extracción y Java. No hace falta Docker. Desde esta carpeta:

```bash
python3 scripts/setup_flink.py
export FLINK_HOME="$PWD/.runtime/flink-2.1.3"
# Configurar JAVA_HOME al JDK elegido antes de ejecutar Flink.
```

El instalador descarga desde Apache y Maven Central, comprueba el SHA512 de la distribución y el SHA1 publicado para el jar en Maven, y extrae sólo dentro de `.runtime/`. Los checksums de Maven verifican integridad del archivo; la descarga se hace por HTTPS. No sube binarios a Git. Si la versión migra a archivo histórico, usar la distribución correspondiente de Apache Archive y revisar su checksum, sin cambiar de rama al azar.

## A · Primer resultado verificable, sin broker

```bash
python3 scripts/flink_bounded.py
```

Usa el SQL [01_bounded_windows.sql](../sql/local/01_bounded_windows.sql), un MiniCluster local y un filesystem source **finito**. Son tres eventos: dos de `a` en el primer minuto con montos 10 y 20; uno de `b` en el siguiente con monto 50. La salida debe tener exactamente `(a, 2, 30)` y `(b, 1, 50)`. El runner valida esa igualdad y guarda SQL, logs y `report.json` bajo `artifacts/flink-bounded-*`.

Flink real ejecuta la ventana, no el simulador Python. Al terminar la fuente finita se pueden cerrar sus ventanas; en un Kafka source infinito hay que analizar el avance de watermarks.

## B · Analítica sobre Kafka real

Abrí tres terminales desde `estudio-eventos/`, con JAVA_HOME/KAFKA_HOME configurados si no se detectan desde la carpeta padre.

**Terminal 1:** broker sandbox aislado hasta Ctrl+C:

```bash
python3 scripts/kafka_demo.py --serve-only
```

**Terminal 2:** publicar una única vez el fixture de analítica:

```bash
python3 scripts/publish_fixture.py --case ml
```

**Terminal 3:** iniciar Flink y abrir el SQL Client:

```bash
export FLINK_HOME="$PWD/.runtime/flink-2.1.3"
"$FLINK_HOME/bin/start-cluster.sh"
"$FLINK_HOME/bin/sql-client.sh"
```

Pegá una por una las sentencias de [02_risk_windows.sql](../sql/local/02_risk_windows.sql). El SELECT continuo muestra observaciones y monto por entidad/minuto. Hay 80 observaciones únicas; el publicador quita redeliveries para que este ejercicio aísle el comportamiento de ventanas. En otro ejercicio, implementá dedup dentro de Flink y revisá su estado/TTL.

El publicador agrega un **marcador de fixture** posterior al último evento que no participa del agregado; hace avanzar event time para observar todas las ventanas. Es una técnica sólo para probar datos sintéticos. No publicar timestamps futuros fabricados para cerrar ventanas de un sistema real. Sin nuevos eventos/fin de input, las últimas ventanas de un stream pueden permanecer abiertas.

## C · Marketing sobre Kafka/Flink

Con el mismo sandbox, publicar una vez:

```bash
python3 scripts/publish_fixture.py --case marketing
```

En otra sesión del SQL Client ejecutar [03_marketing_windows.sql](../sql/local/03_marketing_windows.sql). El publicador normaliza y filtra consentimiento; el SQL deduplica por event_id o transaction_id para compras backend. No sumar monedas diferentes. La identidad de sesión en COUNT DISTINCT es el par client_id/session_id.

El SQL calcula métricas; no implementa las acciones/timers del motor Python. Además, Flink emite watermarks periódicamente por partición: si consumís todo un backlog rápidamente, una compra que el simulador global marca tardía puede procesarse antes del primer watermark. No esperes igualdad automática de tardanza entre ambos motores. Para probar tardanza en Flink, publicá por etapas y observá el watermark antes de enviar el registro atrasado.

## Verificar y detener

Para repetir la verificación automatizada de **Kafka → Flink** sin iniciar servicios a mano:

```bash
python3 scripts/flink_kafka_check.py
```

Inicia su propio broker temporal y consulta snapshots Kafka acotados a los offsets existentes al comenzar. Usa modo **batch** para comparar exactamente 80 observaciones de riesgo y 12 eventos de marketing deduplicados con ingreso USD 195. Este último incluye la compra que el simulador streaming dejó para reconciliación. Verifica conectividad, parsing, dedup y agregados; no mide latencia ni demuestra política de tardanza en un stream continuo. Guarda los SQL exactos y resultados en `artifacts/flink-kafka-*`.

Guardar `EXPLAIN`, schema, watermark observado y resultados. Para un ensayo posterior con checkpoint/restore, configurar un directorio de checkpoint persistente propio y verificar las garantías del sink. Habilitar checkpointing solo no demuestra recuperación ni idempotencia del ejecutor.

Detené el SELECT y cerrá el cliente. Si el cluster lo iniciaste para este ejercicio:

```bash
"$FLINK_HOME/bin/stop-cluster.sh"
```

Luego Ctrl+C en la terminal del broker sandbox. Para repetir sin acumular entradas, iniciar un sandbox nuevo con directorio propio. No usar scripts de parada de otro cluster compartido.

Problemas frecuentes: `Could not find factory kafka` indica jar ausente/incompatible; `Connection refused` suele ser broker/puerto incorrecto; ventanas sin salida requieren revisar event time/watermark; errores de Java requieren comprobar versión y configuración. Confluent Cloud administra catálogo/conectores de otra forma: no pegar sin adaptar el DDL local allí.
