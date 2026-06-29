from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta  # <-- добавил timedelta

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'fill_cdm_settlement_report',
    schedule_interval='0 0 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args
)

fill_report = PostgresOperator(
    task_id='fill_dm_settlement_report',
    postgres_conn_id='pg_connection',
    sql="""
        INSERT INTO cdm.dm_settlement_report (
            restaurant_id,
            restaurant_name,
            settlement_date,
            orders_count,
            orders_total_sum,
            orders_bonus_payment_sum,
            orders_bonus_granted_sum,
            order_processing_fee,
            restaurant_reward_sum
        )
        SELECT 
            o.restaurant_id,
            r.restaurant_name,
            t.ts::date AS settlement_date,
            COUNT(DISTINCT o.id) AS orders_count,
            SUM(f.total_sum) AS orders_total_sum,
            SUM(f.bonus_payment) AS orders_bonus_payment_sum,
            SUM(f.bonus_grant) AS orders_bonus_granted_sum,
            SUM(f.total_sum) * 0.25 AS order_processing_fee,
            SUM(f.total_sum) - SUM(f.bonus_payment) - (SUM(f.total_sum) * 0.25) AS restaurant_reward_sum
        FROM dds.dm_orders o
        JOIN dds.dm_timestamps t ON t.id = o.timestamp_id
        JOIN dds.fct_product_sales f ON f.order_id = o.id
        JOIN dds.dm_restaurants r ON r.id = o.restaurant_id
        WHERE o.order_status = 'CLOSED'
        GROUP BY o.restaurant_id, r.restaurant_name, t.ts::date
        ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
            orders_count = EXCLUDED.orders_count,
            orders_total_sum = EXCLUDED.orders_total_sum,
            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
            order_processing_fee = EXCLUDED.order_processing_fee,
            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
    """,
    dag=dag
)

fill_report