from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json

# В течение всего спринта понял, что с трудом понимаю тему. Если с SQL-скриптами более-менее проблем нет,
# то Python-коды заставили страдать. Оставлю вопросы прямо в коде. Постараюсь объяснить, что я делаю, если верно всё понимаю.

API_BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net' # пишу ссылку, к которой буду обращаться
API_HEADERS = { # мои данные для подключения
    'X-Nickname': 'dlgvzvs',
    'X-Cohort': '15',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

def load_couriers(): # скачиваю справочник курьеров и закидываю его в staging
    hook = PostgresHook(postgres_conn_id='pg_connection') # говорю питону подключиться к соединению под названием pg_connection
    conn = hook.get_conn() # устанавливаю соединение
    cursor = conn.cursor() # создаю курсор для навигации между строк
    
    url = f"{API_BASE_URL}/couriers?limit=50" # уточняю интересующий адрес страницы (на основе имеющейся ссылки)
    
    response = requests.get(url, headers=API_HEADERS) 
    # почему нужен get, а не post, если я передаю мои заголовки, а не скачиваю их откуда-то к себе?
    data = response.json() # конвертирую полученную информацию в объект json
    
    for row in data: # работаю со строками таблицы, чтобы заполнить их
        cursor.execute("""
            INSERT INTO stg.api_couriers (courier_id, courier_name)
            VALUES (%s, %s)
            ON CONFLICT (courier_id) DO NOTHING;
        """) # в полученном json-файле 2 колонки. всё, что в первой автоматически признаётся как courier_id,
             # а остальное - courier_name. повторы пропускаются.
    
    conn.commit() # сохраняю изменения в таблице
    cursor.close() # закрываю курсор
    conn.close() # отсоединяюсь

def load_deliveries(): # cкачиваю данные по доставкам и тоже отправляю в staging
    hook = PostgresHook(postgres_conn_id='pg_connection') # тут всё по аналогии с предыдущей функцией
    conn = hook.get_conn()
    cursor = conn.cursor()

    url = f"{API_BASE_URL}/deliveries?limit=50" # открываю страницу с данными по доставкам

    response = requests.get(url, headers=API_HEADERS)
    data = response.json()
    
    for row in data:
        cursor.execute("""
            INSERT INTO stg.api_deliveries (
            order_id, order_ts, delivery_id, courier_id,
            address, delivery_ts, rate, sum, tip_sum
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (delivery_id) DO NOTHING;
        """) # тут уже побольше столбцов, раскидываю данные по порядку колонок, повторы пропускаю
    
    # Почему задачу по заполнению слоя Staging выполняет не SQL, а Python, 
    # если внутри cursor.execute у нас самый обычный SQL-запрос, с помощью которого и происходит заполнение
    # таблиц в схеме stg?

    conn.commit()
    cursor.close()
    conn.close() # всё аналогично предыдущей функции

# после описания функций, асколько я понимаю, идёт создание ДАГа

default_args = { # описываю стандартные настройки
    'owner': 'dlgvzvs', # мой логин
    'depends_on_past': False, # даг не зависит от прошлых дагов (или чего-то там ещё)
    'retries': 3, # после трёх попыток даг сворачивается
    'retry_delay': timedelta(minutes=5), # при неудаче запускается через 5 минут, а не сразу
}

dag = DAG ( # задаю DAG
    'dm_courier_ledger_dag', # имя дага, к-е будет отображаться в Airflow
    default_args=default_args, # ссылаюсь на параметры (заданы чуть выше)
    description='Загрузка курьерских данных и витрины', # описание
    schedule_interval='@daily', # график запуска дага
    start_date=datetime(2026, 6, 1), # старт 1 июня 2026 года
    catchup=False, # старые данные не восстанавливать
)

# теперь пришла пора прописывать операторы для выозова функций, которые я описал в самом начале

load_couriers = PythonOperator ( # пишем оператор, чтобы вызвать функцию и отследить ход её выполнения
    task_id = 'load_couriers', # имя задачи, которое будет отображаться в Airflow
    python_callable = load_couriers, # непосредственно вызов функции load_couriers
    dag = dag # связываю функцию с дагом
)

load_deliveries = PythonOperator ( # аналогичный предыдущему оператор
    task_id = 'load_deliveries',
    python_callable = load_deliveries,
    dag = dag
)

# Теперь не обращаюсь к написанным выше функциям, вся работа будет в SQL

fill_couriers = PostgresOperator( # заполняю слой DDS данными, которые ранее поместили в STG
    task_id='fill_couriers',
    postgres_conn_id='pg_connection', # здесь передаю имя подключения в Airflow
    sql="""
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        SELECT
	        ac.courier_id,
	        ac.courier_name
        FROM stg.api_couriers ac
        ON CONFLICT(courier_id) DO nothing;
    """, # вношу ID (varchar) и имя курьера, к ним присоединится новый суррогатный ключ id (serial)
    dag=dag
)

fill_deliveries = PostgresOperator ( # заполняю таблицу фактов
    task_id='fill_deliveries',
    postgres_conn_id='pg_connection', # работаю в том же соединении
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
    """, # оставляю только ссылки на ID доставок, курьеров и заказов, а также измеримые показатели, так как всю остальную информацию можно узнать в таблицах измерений.
    dag=dag
)

fill_mart = PostgresOperator ( # заполняю итоговую витрину
    task_id='fill_mart',
    postgres_conn_id='pg_connection',
    sql="""
    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count,
orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
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

[load_couriers, load_deliveries] >> fill_couriers >> fill_deliveries >> fill_mart # задаю порядок функций

# на этом вроде всё, можно запускаться