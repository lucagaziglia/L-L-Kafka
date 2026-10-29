-- Apache Flink local. Input canónico (consentimiento/allowlist/validación ya aplicados).
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.source.idle-timeout' = '10 s';

CREATE TABLE marketing_raw (
  event_id STRING,
  client_id STRING,
  session_id STRING,
  `event` STRING,
  timestamp_ms BIGINT,
  `source` STRING,
  campaign STRING,
  transaction_id STRING,
  `value` DOUBLE,
  currency STRING,
  event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'll.marketing.canonical.v1',
  'properties.bootstrap.servers' = '127.0.0.1:19092',
  'properties.group.id' = 'll-flink-marketing-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE VIEW marketing_dedup AS
SELECT event_id, client_id, session_id, `event`, `source`, campaign,
       transaction_id, `value`, currency, event_time
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY CASE WHEN `event` = 'purchase' AND `source` = 'orders-service'
      THEN CONCAT('transaction:', transaction_id) ELSE CONCAT('event:', event_id) END
    ORDER BY event_time ASC
  ) AS rn
  FROM marketing_raw
  WHERE `event` IN ('page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
) WHERE rn = 1;

SELECT window_start, window_end, campaign, currency,
       COUNT(*) AS events,
       COUNT(DISTINCT ROW(client_id, session_id)) AS observed_sessions,
       SUM(CASE WHEN `event` = 'purchase' AND `source` = 'orders-service'
           THEN `value` ELSE 0 END) AS backend_revenue
FROM TABLE(TUMBLE(TABLE marketing_dedup, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, campaign, currency;
