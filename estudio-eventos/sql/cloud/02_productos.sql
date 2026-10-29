-- Confluent Cloud. Requiere ll_unique_orders_v1 y datasets del workshop.
DESCRIBE payments;
SHOW CREATE TABLE customers;
SHOW CREATE TABLE products;

SELECT o.order_id, o.customer_id, o.product_id,
       o.`$rowtime` AS order_time, p.payment_time, p.amount
FROM ll_unique_orders_v1 AS o
JOIN payments AS p ON o.order_id = p.order_id
 AND o.`$rowtime` BETWEEN p.payment_time - INTERVAL '10' MINUTE AND p.payment_time;

-- Inspeccionar tipos y watermarks de la fuente antes de crear.
SET 'client.statement-name' = 'll-valid-orders-v1';
CREATE TABLE ll_valid_orders_v1 (
  order_id STRING,
  customer_id INT,
  product_id STRING,
  order_time TIMESTAMP_LTZ(3),
  payment_time TIMESTAMP_LTZ(3),
  amount DECIMAL(18, 2),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) AS
SELECT o.order_id, o.customer_id, o.product_id, o.`$rowtime`,
       p.payment_time, CAST(p.amount AS DECIMAL(18, 2))
FROM ll_unique_orders_v1 AS o
JOIN payments AS p ON o.order_id = p.order_id
 AND o.`$rowtime` BETWEEN p.payment_time - INTERVAL '10' MINUTE AND p.payment_time;

-- Dimensiones versionadas con PK/event-time compatibles: verificar SHOW CREATE TABLE.
SELECT o.order_id, o.customer_id, c.name AS customer_name,
       p.brand, p.name AS product_name, o.amount, o.order_time
FROM ll_valid_orders_v1 AS o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
  ON o.customer_id = c.customer_id
JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p
  ON o.product_id = p.product_id;

-- Agregado que se actualiza. Si se materializa, elegir sink/changelog compatible.
SELECT customer_id, SUM(amount) AS observed_paid_amount,
       CASE WHEN SUM(amount) >= 500 THEN 'gold' ELSE 'standard' END AS loyalty_level
FROM ll_valid_orders_v1
GROUP BY customer_id;
