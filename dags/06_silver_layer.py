from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_process import process_bronze_to_silver

default_args = {
    'owner': 'Lorenzo',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='06_silver_transformer',
    default_args=default_args,
    description='ETL: converts json from bronze to silver',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 * * * *', # hourly
    catchup=False,
    tags=['silver', 'etl', 'pandas']
) as dag:

    transform_task = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=process_bronze_to_silver
    )