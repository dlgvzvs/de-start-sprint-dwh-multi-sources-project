import logging
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import requests
import json
import psycopg2
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

nickname = 'dlgvzvs'
cohort = '15'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
    }


conn = psycopg2.connect(
    dbname='de',
    user='jovyan',
    password='jovyan',
    host='localhost',
    port='5432'
)



def get_couriers_from_api(sort_field='_id', sort_direction='asc', limit ='50', offset='0'): # добавил limit в параметрах
    params = {
    'sort_field': sort_field,
    'sort_direction': sort_direction,
    'limit': str(limit),
    'offset': str(offset)
    }
    couriers_url = base_url+'couriers'
    response = requests.get(couriers_url, headers=headers, params=params)
    return response.json()

def couriers_from_api_to_stg():
    offset = 0 # смещение (в первый раз стартуем с нулевого индекса, т.е. с самого начала)
    limit = 50 # только 50 записей за раз
    while True: # тут с трудом понимаю, но вроде это цикл, который заканчивается либо командой break, либо когда имеющиеся данные заканчиваются
        couriers_list = get_couriers_from_api(offset=offset, limit=limit)
        if not couriers_list: # если данные отсутствуют, бросаем это дело
            break
        for courier_json in couriers_list: # если же есть, с чем работать, приступаем к заполнению
            courier_id = courier_json['_id']
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO stg.api_couriers (object_id, object_value)
                        VALUES(%(courier_id)s, %(courier_json)s::json)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_id = EXCLUDED.object_id,
                            object_value = EXCLUDED.object_value;
                    """,{
                            "courier_id": courier_id,
                        "courier_json": json.dumps(courier_json, ensure_ascii=False)
                        }
                            )    
        offset += limit # перехожу к записи с индексом 50
    conn.commit()



def get_deliveries_from_api(restaurant_id=None, from_date=None, to_date=None, sort_field='_id'):
    params = {
    'restaurant_id': restaurant_id,
    'from_date': from_date, 
    'to_date': to_date,
    'sort_field': sort_field
    }
    deliveries_url = base_url+'deliveries'
    response = requests.get(deliveries_url, headers=headers, params=params)
    return response.json()

def deliveries_from_api_to_stg():
    offset = 0 # такая же штука, как в предыдущей функции
    limit = 50
    # формат даты взял из примера в base_url
    to_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    while True:
        deliveries_list= get_deliveries_from_api(from_date, to_date, limit=50, offset=0) # гружу записи за последнюю неделю по 50 записей за раз
        if not deliveries_list:
            break
        for delivery_json in deliveries_list:
            delivery_id = delivery_json['delivery_id']
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO stg.api_deliveries (object_id, object_value)
                    VALUES(%(delivery_id)s, %(delivery_json)s::json)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_id = EXCLUDED.object_id,
                        object_value = EXCLUDED.object_value;
                    """, {
                            "delivery_id": delivery_id,
                            "delivery_json": json.dumps(delivery_json, ensure_ascii=False)
                        })
        offset += limit
    conn.commit()


dag = DAG(dag_id='data_from_api_to_stg_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['form couriers and deliveries api to stg'],
    is_paused_upon_creation=False)

task_1 = PythonOperator(task_id='couriers_from_api_to_stg',
                      python_callable=couriers_from_api_to_stg,
                      dag=dag)   

task_2 = PythonOperator(task_id='deliveries_from_api_to_stg',
                      python_callable=deliveries_from_api_to_stg,
                      dag=dag)   

task_1 >> task_2

# Остался вопрос по этому дагу. Нет понимания, для чего он. Его писал не я, в задании про него, насколько я помню, не было сказано.
# Всю свою работу (в первой версии проекта) я выполнял исключительно в файле dm_courier_ledger_dag.py.
# Мы где-то ссылаемся на результаты работы дага data_from_api_dag.py?