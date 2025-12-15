from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import yfinance as yf
import json
import logging
import pandas as pd
import os

BUCKET_NAME = "quantstream-lake-lorenzo-2026"

default_args = {
    'owner': 'Lorenzo',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def get_portfolio_assets():
    """
    Connects to docker using connection set up in the ui and returns symbol + type
    """
    # hook to avoid manual connect
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # only DISTINCT (to avoid if we bought btc 100 times)
    sql = "SELECT DISTINCT symbol, asset_type FROM my_portfolio;"
    
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    assets = cursor.fetchall()
    
    # parse to dict
    asset_list = [{'symbol': row[0], 'type': row[1]} for row in assets]
    
    logging.info(f"💰 list of assets found: {asset_list}")
    return asset_list

def fetch_prices_and_save(**context):
    """
    get our assets and find the up-to-date prices
    """
    assets = get_portfolio_assets()
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    if not assets:
        logging.warning("⚠️ Empty portfolio. load sth!")
        return

    for asset in assets:
        symbol = asset['symbol']
        asset_type = asset['type']
        price = None
        
        try:
            # LOGIC !!!! REVIEW AGAIN 
            if asset_type == 'CRYPTO':
                # Binance API
                pair = f"{symbol}USDT"
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={pair}"
                response = requests.get(url)
                data = response.json()
                price = float(data['price'])
                logging.info(f" Crypto {symbol}: ${price} (Vía Binance)")
                
            elif asset_type == 'STOCK':
                # --- alpha vantage api
                # alternative to yahoo
                
                API_KEY = os.getenv('ALPHAVANTAGE_KEY')
                
                try:
                    # Endpoint: GLOBAL_QUOTE (actual price and last price)
                    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}"
                    
                    logging.info(f"Stock {symbol}...")
                    response = requests.get(url)
                    data = response.json()
                    
                    # 
                    if "Global Quote" in data and "05. price" in data["Global Quote"]:
                        price = float(data["Global Quote"]["05. price"])
                        logging.info(f"✅ Stock {symbol}: ${price} (via Alpha Vantage API)")
                    
                    # limits
                    elif "Note" in data:
                        logging.warning(f"⚠️ max limit reached {symbol}. waiting...")
                        raise ValueError("API Limit Reached")
                    else:
                        logging.error(f"❌ unexpected  {symbol}: {data}")
                        raise ValueError("Empty Data")

                except Exception as e:
                    logging.error(f"❌ Error buscando {symbol}: {e}")
                    # Si falla la API real, mantenemos el fallback solo por seguridad
                    if symbol == 'AAPL': price = 225.50
                    else: price = 100.00
                    logging.warning(f"⚠️ Usando precio fallback por error de API.")
            
            # saves in s3 bronze layer
            if price:
                payload = {
                    "symbol": symbol,
                    "price": price,
                    "asset_type": asset_type,
                    "ingestion_timestamp": datetime.utcnow().isoformat()
                }
                
                # structure the folder with types
                date_part = datetime.utcnow().strftime('%Y-%m-%d')
                file_name = f"bronze/market_data/{asset_type.lower()}/{symbol}/date={date_part}/data_{int(datetime.utcnow().timestamp())}.json"
                
                s3_hook.load_string(
                    json.dumps(payload),
                    key=file_name,
                    bucket_name=BUCKET_NAME,
                    replace=True
                )
                
        except Exception as e:
            logging.error(f"❌ Error symbol: {symbol}: {e}")
            continue # if any fails, we keep looking the next one

with DAG(
    dag_id='05_dynamic_portfolio_update',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['portfolio', 'dynamic', 'production']
) as dag:

    update_prices_task = PythonOperator(
        task_id='fetch_portfolio_prices',
        python_callable=fetch_prices_and_save
    )