import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable

from lib import ConnectionBuilder, MongoConnect
from stg_order_system_users_dag.user_reader import UserReader
from stg_order_system_users_dag.user_loader import UserLoader
from stg_order_system_users_dag.pg_saver import PgSaver

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'ordersystem', 'users'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_order_system_users():
    
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")
    
    @task()
    def load_users():
        last_load_ts = Variable.get("USERS_WATERMARK", default_var=None)
        
        if last_load_ts is None:
            last_load_ts = pendulum.datetime(1970, 1, 1, tz="UTC")
            log.info("Первый запуск, загружаем все данные")
        else:
            last_load_ts = pendulum.parse(last_load_ts)
            
        log.info(f"Загрузка пользователей с update_ts > {last_load_ts}")
        
        mongo_connect = MongoConnect(
            cert_path=cert_path,
            user=db_user,
            pw=db_pw,
            host=host,
            rs=rs,
            auth_db=db,
            main_db=db
        )
        
        reader = UserReader(
            mc=mongo_connect,
            last_load_ts=last_load_ts
        )
        
        pg_saver = PgSaver(dwh_pg_connect)
        
        loader = UserLoader(
            reader=reader,
            pg_saver=pg_saver,
            log=log
        )
        
        max_update_ts = loader.run_copy()
        
        if max_update_ts:
            Variable.set("USERS_WATERMARK", str(max_update_ts))
            log.info(f"USERS_WATERMARK обновлен на {max_update_ts}")
    
    load_users()


dag = sprint5_example_stg_order_system_users()