-- Confluent Cloud: ejecutar una sentencia por celda en el catálogo/base del workshop.
-- Adaptación educativa de la revisión indicada en docs/FUENTES.md.
SHOW TABLES;
DESCRIBE orders;
DESCRIBE EXTENDED orders;
SHOW CREATE TABLE orders;

SELECT order_id, product_id, customer_id, price, `$rowtime`
FROM orders LIMIT 10;

SELECT order_id, COUNT(*) AS occurrences
FROM orders GROUP BY order_id;

EXPLAIN
SELECT order_id, product_id, customer_id, price
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY `$rowtime` ASC
  ) AS rn FROM orders
) WHERE rn = 1;

SET 'sql.state-ttl' = '1 hour';
SET 'client.statement-name' = 'll-unique-orders-v1';
CREATE TABLE ll_unique_orders_v1 AS
SELECT order_id, product_id, customer_id, price
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY `$rowtime` ASC
  ) AS rn FROM orders
) WHERE rn = 1;

SHOW CREATE TABLE ll_unique_orders_v1;

SELECT window_start, window_end, window_time,
       COUNT(DISTINCT order_id) AS unique_orders, SUM(price) AS observed_amount
FROM TABLE(TUMBLE(TABLE ll_unique_orders_v1, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, window_time;

SELECT window_start, window_end, window_time, COUNT(DISTINCT order_id) AS unique_orders
FROM TABLE(HOP(TABLE ll_unique_orders_v1, DESCRIPTOR(`$rowtime`),
               INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
GROUP BY window_start, window_end, window_time;
