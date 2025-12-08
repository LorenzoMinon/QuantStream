from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import json
import logging

# config
BUCKET_NAME = "quantstream-lake-lorenzo-2026"

default_args = {
    'owner': 'Lorenzo',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def fetch_and_save_bitcoin():
    # 1. extract. save to json > s3
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url)
        data = response.json()
        price = data['price']
        
        # timestamp
        data['ingestion_timestamp'] = datetime.utcnow().isoformat()
        
        # "Hive Partitioning"
        date_part = datetime.utcnow().strftime('%Y-%m-%d')
        file_name = f"bronze/crypto/binance/ticker/date={date_part}/btc_{int(datetime.utcnow().timestamp())}.json"

        # S3Hook > official Airflow tool for aws
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=file_name,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        
        logging.info(f"✅ SAVED FILE://{BUCKET_NAME}/{file_name}")

    except Exception as e:
        logging.error(f"❌ ERROR: {e}")
        raise e

with DAG(
    dag_id='02_bitcoin_to_s3_bronze', # new one
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ingest', 's3', 'bronze']
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_and_save_btc',
        python_callable=fetch_and_save_bitcoin
    )