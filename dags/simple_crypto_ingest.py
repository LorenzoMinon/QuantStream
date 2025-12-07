from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging

# 1. DAG
default_args = {
    'owner': 'Lorenzo',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# 2. Extract with fetch
def fetch_bitcoin_price():
    """
    public api
    """
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url)
        data = response.json()
        price = data['price']
        
        # logs
        logging.info(f"🚀 PRECIO ACTUAL DE BITCOIN: ${price} USDT")
        return price
    except Exception as e:
        logging.error(f"❌ Error conectando a Binance: {e}")
        raise e

# 3 Pipeline
with DAG(
    dag_id='01_simple_bitcoin_ingest',
    default_args=default_args,
    schedule_interval='@daily',  # daily
    catchup=False,               # from now on, dont autofill past days
    tags=['ingest', 'crypto', 'quantstream']
) as dag:

    # Task 1
    get_price_task = PythonOperator(
        task_id='fetch_btc_price',
        python_callable=fetch_bitcoin_price
    )
    
    get_price_task