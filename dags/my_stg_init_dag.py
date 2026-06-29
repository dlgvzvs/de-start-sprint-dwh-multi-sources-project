from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'jovyan',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'my_stg_init_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_users = PostgresOperator(
        task_id='create_bonussystem_users',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
        CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
            id INTEGER NOT NULL,
            order_user_id TEXT NOT NULL
        );
        """
    )

    create_events = PostgresOperator(
        task_id='create_bonussystem_events',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
        CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
            id INTEGER NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            event_type VARCHAR NOT NULL,
            event_value TEXT NOT NULL
        );
        """
    )

    create_ranks = PostgresOperator(
        task_id='create_bonussystem_ranks',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
        CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
            id INTEGER NOT NULL,
            name VARCHAR(2048) NOT NULL,
            bonus_percent NUMERIC(19,5) NOT NULL,
            min_payment_threshold NUMERIC(19,5) NOT NULL
        );
        """
    )

    [create_users, create_events, create_ranks]