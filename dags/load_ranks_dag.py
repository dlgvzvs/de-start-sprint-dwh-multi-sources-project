from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import json

def load_ranks():
    source_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    target_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    rows = source_hook.get_records("SELECT id, name, bonus_percent, min_payment_threshold FROM ranks")
    for row in rows:
        target_hook.run("""
            INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                bonus_percent = EXCLUDED.bonus_percent,
                min_payment_threshold = EXCLUDED.min_payment_threshold
        """, parameters=row)

def load_users():
    source_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    target_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    rows = source_hook.get_records("SELECT id, order_user_id FROM users")
    for row in rows:
        target_hook.run("""
            INSERT INTO stg.bonussystem_users (id, order_user_id)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET
                order_user_id = EXCLUDED.order_user_id
        """, parameters=row)

def load_events():
    source_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    target_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    
    # Получаем последний загруженный id (как в эталонном коде)
    cursor_row = target_hook.get_records("""
        SELECT workflow_settings FROM stg.srv_wf_settings 
        WHERE workflow_key = 'events_origin_to_stg_workflow'
    """)
    
    last_loaded_id = -1
    if cursor_row:
        last_loaded_id = int(cursor_row[0][0].get('last_loaded_id', -1))
    
    # Загружаем события (как в эталонном коде)
    rows = source_hook.get_records("""
        SELECT id, event_ts, event_type, event_value 
        FROM outbox 
        WHERE id > %s 
        ORDER BY id ASC
    """, parameters=(last_loaded_id,))
    
    if not rows:
        print("Нет новых событий для загрузки")
        return
    
    # Сохраняем и обновляем курсор
    with target_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                    VALUES (%s, %s, %s, %s)
                """, (row[0], row[1], row[2], row[3]))
            
            # Используем max() как в эталонном коде
            new_last_loaded_id = max([row[0] for row in rows])
            
            cur.execute("""
                INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                VALUES ('events_origin_to_stg_workflow', %s)
                ON CONFLICT (workflow_key) DO UPDATE SET
                    workflow_settings = EXCLUDED.workflow_settings
            """, (json.dumps({'last_loaded_id': new_last_loaded_id}),))
            
            conn.commit()
    
    print(f"Загружено {len(rows)} событий. Последний id: {new_last_loaded_id}")

dag = DAG(
    dag_id='load_ranks_old_style',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'load_ranks']
)

load_ranks_task = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks,
    dag=dag
)

load_users_task = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=dag
)

load_events_task = PythonOperator(
    task_id='load_events',
    python_callable=load_events,
    dag=dag
)

load_ranks_task >> load_users_task >> load_events_task