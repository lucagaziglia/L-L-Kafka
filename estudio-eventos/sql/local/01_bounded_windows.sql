-- Apache Flink 2.1.x. El runner reemplaza __INPUT__ y __OUTPUT__ por rutas absolutas.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.target' = 'local';
SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '1';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE observations (
  event_id STRING,
  entity_id STRING,
  event_time_ms BIGINT,
  amount DOUBLE,
  event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '__INPUT__',
  'format' = 'json'
);

CREATE TABLE window_metrics (
  entity_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  observations BIGINT,
  total_amount DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '__OUTPUT__',
  'format' = 'json'
);

INSERT INTO window_metrics
SELECT entity_id, window_start, window_end, COUNT(*), SUM(amount)
FROM TABLE(TUMBLE(TABLE observations, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY entity_id, window_start, window_end;
