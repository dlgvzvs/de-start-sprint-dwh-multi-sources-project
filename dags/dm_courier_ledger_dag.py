from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json

API_BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
API_HEADERS = {
    'X-Nickname': 'dlgvzvs',
    'X-Cohort': '15',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

def load_couriers():
    hook = PostgresHook(postgres_conn_id='pg_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    offset=0 # задаю ограничения
    limit=50
    
    url = f"{API_BASE_URL}/couriers" # тут убрал лимит, потому что и так грузим только по 50 записей за раз
    
    while True:
        response = requests.get(url, headers=API_HEADERS, params = {'limit': limit, 'offset': offset}) # здесь передаю ограничения в get-запрос
        data = response.json()
        if not data: # если данных нет, бросаю
            break
        for row in data:
            cursor.execute("""
                INSERT INTO stg.api_couriers (courier_id, courier_name)
                VALUES (%s, %s)
                ON CONFLICT (courier_id) DO NOTHING;
            """, (row['_id'], row['name'])) # закидываю полученные данные в соответствующие ячейки
        offset += limit # тут возник тупняк с отступами
    
    conn.commit()
    cursor.close()
    conn.close()

def load_deliveries():
    hook = PostgresHook(postgres_conn_id='pg_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    offset=0 # всё аналогично предыдущей функции
    limit=50

    url = f"{API_BASE_URL}/deliveries" # убираю лимит из урл

    while True:
        to_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # для справочника доставок задаю ограничения по датам
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        response = requests.get(url, headers=API_HEADERS, params={'limit': limit, 'offset': offset, 'from': from_date, 'to': to_date}) # передаю ограничения в get-запрос
        data = response.json()
        if not data:
            break
        for row in data:
            cursor.execute("""
                INSERT INTO stg.api_deliveries (
                order_id, order_ts, delivery_id, courier_id,
                address, delivery_ts, rate, sum, tip_sum
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (delivery_id) DO NOTHING;
            """, (
                row['order_id'],
                row['order_ts'],
                row['delivery_id'],
                row['courier_id'],
                row['address'],
                row['delivery_ts'],
                    row['rate'],
                row['sum'],
                row['tip_sum']
            ))
        offset += limit   

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'dlgvzvs',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG (
    'dm_courier_ledger_dag',
    default_args=default_args,
    description='Загрузка курьерских данных и витрины',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
)

load_couriers = PythonOperator (
    task_id = 'load_couriers',
    python_callable = load_couriers,
    dag = dag
)

load_deliveries = PythonOperator (
    task_id = 'load_deliveries',
    python_callable = load_deliveries,
    dag = dag
)

fill_couriers = PostgresOperator(
    task_id='fill_couriers',
    postgres_conn_id='pg_connection',
    sql="""
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        SELECT
	        ac.courier_id,
	        ac.courier_name
        FROM stg.api_couriers ac
        ON CONFLICT(courier_id) DO nothing;
    """,
    dag=dag
)

fill_deliveries = PostgresOperator (
    task_id='fill_deliveries',
    postgres_conn_id='pg_connection',
    sql="""
    INSERT INTO dds.fct_deliveries (delivery_id, courier_id, order_id, rate, tip_sum)
    SELECT
	    ad.delivery_id AS delivery_id,
	    ac.id AS courier_id,
	    ad.order_id AS order_id,
	    ad.rate AS rate,
	    ad.tip_sum AS tip_sum
    FROM stg.api_deliveries ad
    LEFT JOIN dds.dm_couriers dc ON dc.courier_id = ad.courier_id
    LEFT JOIN dds.dm_orders do ON do.order_key = ad.order_id;
    """,
    dag=dag
)

[load_couriers, load_deliveries] >> fill_couriers >> fill_deliveries