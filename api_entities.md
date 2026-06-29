# Проект: витрина для курьеров

## 1. Какие поля нужны в витрине?
- courier_id
- courier_name
- settlement_year
- settlement_month
- orders_count
- orders_total_sum
- rate_avg
- order_processing_fee
- courier_order_sum
- courier_tips_sum
- courier_reward_sum

## 2. Какие таблицы уже есть в DDS?
- dds.dm_orders (заказы)
- dds.fct_product_sales (продажи)

## 3. Какие данные нужно догрузить из API?
- Курьеры (ID, имя)
- Доставки (заказ, курьер, рейтинг, чаевые)

## 4. Какие новые таблицы создать?
- stg.api_couriers
- stg.api_deliveries
- dds.dm_couriers
- dds.fct_deliveries
- cdm.dm_courier_ledger