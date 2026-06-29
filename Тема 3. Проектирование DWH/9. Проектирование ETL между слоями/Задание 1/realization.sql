-- 1. Очистка таблиц (сначала дочерние, потом родительские)
DELETE FROM dds.fct_product_sales;
DELETE FROM dds.dm_orders;
DELETE FROM dds.dm_products;
DELETE FROM dds.dm_restaurants;
DELETE FROM dds.dm_users;
DELETE FROM dds.dm_timestamps;
DELETE FROM cdm.dm_settlement_report;

-- 2. Загрузка тестовых данных
COPY dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to) FROM '/data/dm_restaurants_';
COPY dds.dm_products (id, restaurant_id, product_id, product_name, product_price, active_from, active_to) FROM '/data/dm_products_';
COPY dds.dm_timestamps (id, ts, year, month, day, "time", date) FROM '/data/dm_timestamps_';
COPY dds.dm_users (id, user_id, user_name, user_login) FROM '/data/dm_users_';
COPY dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id) FROM '/data/dm_orders_';
COPY dds.fct_product_sales (id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) FROM '/data/fct_product_sales_';


-- 4. Наполнение витрины
INSERT INTO cdm.dm_settlement_report (
    restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, 
    order_processing_fee, restaurant_reward_sum)
SELECT
    r.restaurant_id,
    r.restaurant_name,
    t.date AS settlement_date,
    COUNT(DISTINCT o.id) AS orders_count,
    SUM(ps.total_sum) AS orders_total_sum,
    SUM(ps.bonus_payment) AS orders_bonus_payment_sum,
    SUM(ps.bonus_grant) AS orders_bonus_granted_sum,
    SUM(ps.total_sum) * 0.25 AS order_processing_fee,
    SUM(ps.total_sum) - (SUM(ps.total_sum) * 0.25) - SUM(ps.bonus_payment) AS restaurant_reward_sum
FROM dds.fct_product_sales ps
JOIN dds.dm_orders o ON o.id = ps.order_id
JOIN dds.dm_products p ON p.id = ps.product_id
JOIN dds.dm_timestamps t ON t.id = o.timestamp_id
JOIN dds.dm_users u ON u.id = o.user_id
JOIN dds.dm_restaurants r ON r.id = o.restaurant_id AND r.id = p.restaurant_id
WHERE o.order_status = 'CLOSED'
GROUP BY r.restaurant_id, r.restaurant_name, t.date
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
;