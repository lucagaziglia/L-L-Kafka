-- Apache Flink 2.1.x + flink-sql-connector-kafka 5.0.0-2.1.
-- Broker sandbox 19092; ejecutar scripts/kafka_demo.py --serve-only en otra terminal.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE risk_raw (
  event_id STRING,
  entity_id STRING,
  event_type STRING,
  event_time_ms BIGINT,
  payload ROW<amount DOUBLE, device_risk DOUBLE, attempts_1m INT>,
  event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'll.risk.raw.v1',
  'properties.bootstrap.servers' = '127.0.0.1:19092',
  'properties.group.id' = 'll-flink-risk-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Input de este ejercicio: observations únicas; el publicador elimina redeliveries.
CREATE VIEW risk_observations AS
SELECT entity_id, event_time, payload.amount AS amount
FROM risk_raw
WHERE event_type = 'risk.observed.v1' AND payload.amount >= 0;

-- Consulta continua; cerrar con Ctrl+C después de observar las ventanas.
SELECT entity_id, window_start, window_end, COUNT(*) AS observations, SUM(amount) AS total_amount
FROM TABLE(TUMBLE(TABLE risk_observations, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY entity_id, window_start, window_end;
