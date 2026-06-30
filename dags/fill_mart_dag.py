# Выношу заполнение витрины в отдельный даг, а всё остальное останется в старом файле - dm_courier_ledger_dag.py

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dlgvzvs',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fill_mart_dag',
    default_args=default_args,
    description='Ежемесячное заполнение витрины курьеров',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
)

fill_mart = PostgresOperator (
    task_id='fill_mart',
    postgres_conn_id='pg_connection',
    sql="""
    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
	SELECT
	fd.courier_id AS courier_id,
	dc.courier_name AS courier_name,
	EXTRACT(YEAR FROM do.order_ts) AS settlement_year,
	EXTRACT(MONTH FROM do.order_ts) AS settlement_month,
	COUNT(ad.order_id) AS orders_count,
	SUM(fps.total_sum) AS orders_total_sum,
	AVG(ad.rate) AS rate_avg,
	SUM(fps.total_sum) * 0.25 AS order_processing_fee,
	CASE
		WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
		WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
		WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
		WHEN rate_avg >= 4.9 THEN GREATEST(orders_total_sum * 0.10, 200) END
		AS courier_order_sum,
	SUM(ad.tip_sum) AS courier_tips_sum,
	CASE
		WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
		WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
		WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
		WHEN rate_avg >= 4.9 THEN GREATEST(orders_total_sum * 0.10, 200)
		END + courier_tips_sum * 0.95 AS courier_reward_sum
FROM dds.fct_deliveries fd
JOIN dds.dm_couriers dc ON fd.courier_id = dc.courier_id
JOIN stg.api_deliveries ad ON ad.delivery_id = fd.delivery_id
JOIN dds.dm_orders do ON do.order_key = fd.order_id
JOIN dds.fct_product_sales fps ON fps.order_id = do.id
GROUP BY 
	fd.courier_id,
    dc.courier_name,
    EXTRACT(YEAR FROM do.order_ts),
    EXTRACT(MONTH FROM do.order_ts);
    """,
    dag=dag
)