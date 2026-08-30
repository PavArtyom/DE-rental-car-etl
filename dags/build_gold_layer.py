from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='gold_layer_complete',
    default_args=default_args,
    description='Построение Gold слоя - Star Schema',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'star_schema', 'complete'],
    template_searchpath=['/opt/airflow'],
) as dag:

    build_gold = SQLExecuteQueryOperator(
        task_id='build_gold_tables',
        conn_id='postgres_dwh',
        sql='sql/build_gold_layer.sql',
        split_statements=True,
        autocommit=True,
    )
