from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime
import requests
import json
import logging
import pandas as pd
import io

# config
BUCKET_NAME = "quantstream-lake-lorenzo-2026"
GLUE_CRAWLER_NAME = "quantstream_silver_crawler"


default_args = {
    'owner': 'Lorenzo',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def fetch_bitcoin_raw(**context):
    # downloads json and returns route saved in s3
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    response = requests.get(url)
    data = response.json()
    data['ingestion_timestamp'] = datetime.utcnow().isoformat()
    
    date_part = datetime.utcnow().strftime('%Y-%m-%d')
    file_name = f"bronze/crypto/binance/ticker/date={date_part}/btc_{int(datetime.utcnow().timestamp())}.json"

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(json.dumps(data), file_name, BUCKET_NAME, replace=True)
    
    logging.info(f"✅ Raw guardado: {file_name}")
    
    return file_name

# transform new !
def transform_to_silver(**context):

    #transforms bronze data to Parquet 

    source_key = context['task_instance'].xcom_pull(task_ids='extract_bronze')
    logging.info(f"🔄 Procesando archivo: {source_key}")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # read from s3
    file_content = s3_hook.read_key(source_key, BUCKET_NAME)
    json_content = json.loads(file_content)
    
    # using pandas
    df = pd.DataFrame([json_content]) # 1 row table
    
    # cleaning
    df['price'] = df['price'].astype(float)
    df['symbol'] = df['symbol'].astype(str)
    df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
    
    # parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    
    # new silver "key"
    dest_key = source_key.replace("bronze", "silver").replace(".json", ".parquet")
    
    # upload to s3
    s3_hook.load_bytes(
        parquet_buffer.getvalue(),
        key=dest_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    
    logging.info(f"✨ Silver !: {dest_key}")

with DAG(
    dag_id='04_btc_etl_full_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'crypto', 'glue']
) as dag:

    # Task 1 : ingest
    extract_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=fetch_bitcoin_raw,
        provide_context=True
    )

    # Task 2: transformation
    transform_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_to_silver,
        provide_context=True
    )
    # Task 3: catalog
    trigger_crawler = GlueCrawlerOperator(
        task_id='trigger_glue_crawler',
        config={'Name': GLUE_CRAWLER_NAME},
        aws_conn_id='aws_default',
        wait_for_completion=True # wait to finish
    )
    # define order, first extract and then transform
    extract_task >> transform_task