CREATE DATABASE IF NOT EXISTS shop;

-- Сырые события (event log)
CREATE TABLE IF NOT EXISTS shop.events_raw
(
  event_time DateTime,
  event_date Date DEFAULT toDate(event_time),

  event_id UUID,
  event_type LowCardinality(String), -- page_view, product_view, add_to_cart, checkout_start, payment_attempt, order_created, order_paid, order_cancelled, order_delivered

  user_id UInt64,
  session_id UUID,
  order_id UUID,

  product_id UInt32,
  category LowCardinality(String),
  price Float32,
  quantity UInt16,

  device LowCardinality(String),      -- web/ios/android
  channel LowCardinality(String),     -- organic/ads/email
  country LowCardinality(String),

  status LowCardinality(String),      -- ok/error
  error_code LowCardinality(String),

  latency_ms UInt32
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, event_time, event_type, user_id, session_id, order_id);

-- Факты по заказам (одна строка на заказ)
CREATE TABLE IF NOT EXISTS shop.orders_fact
(
  order_id UUID,
  user_id UInt64,
  created_time DateTime,
  paid_time DateTime,
  delivered_time DateTime,
  cancelled_time DateTime,

  channel LowCardinality(String),
  device LowCardinality(String),
  country LowCardinality(String),

  order_value Float64,
  items UInt64,

  payment_status LowCardinality(String),  -- paid/cancelled/failed
  cancel_reason LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (order_id);

-- Материализованная витрина: из событий собираем order-факт
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.mv_orders_fact
TO shop.orders_fact
AS
SELECT
  order_id,
  any(user_id) AS user_id,
  minIf(event_time, event_type='order_created') AS created_time,
  minIf(event_time, event_type='order_paid') AS paid_time,
  minIf(event_time, event_type='order_delivered') AS delivered_time,
  minIf(event_time, event_type='order_cancelled') AS cancelled_time,

  any(channel) AS channel,
  any(device) AS device,
  any(country) AS country,

  sumIf(price * quantity, event_type IN ('order_created','order_paid','order_delivered')) AS order_value,
  sumIf(quantity, event_type IN ('order_created','order_paid','order_delivered')) AS items,

  multiIf(
    maxIf(1, event_type='order_paid')=1, 'paid',
    maxIf(1, event_type='order_cancelled')=1, 'cancelled',
    maxIf(1, event_type='payment_attempt' AND status='error')=1, 'failed',
    'unknown'
  ) AS payment_status,

  anyIf(error_code, event_type='order_cancelled') AS cancel_reason
FROM shop.events_raw
WHERE order_id != toUUID('00000000-0000-0000-0000-000000000000')
GROUP BY order_id;

-- Для когорт: первый день активности пользователя
CREATE TABLE IF NOT EXISTS shop.user_first_seen
(
  user_id UInt64,
  first_date Date
)
ENGINE = ReplacingMergeTree
ORDER BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS shop.mv_user_first_seen
TO shop.user_first_seen
AS
SELECT
  user_id,
  min(toDate(event_time)) AS first_date
FROM shop.events_raw
GROUP BY user_id;
