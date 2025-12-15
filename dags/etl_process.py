import pandas as pd
import json
import logging
import s3fs
import os

BUCKET_NAME = "quantstream-lake-lorenzo-2026"

def process_bronze_to_silver():


    # keys
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not key or not secret:
        raise ValueError("keys not found")

    # connection to s3 using s3fs
    fs = s3fs.S3FileSystem(key=key, secret=secret)
    
    # look up for bronze files ** glob
    input_path = f"{BUCKET_NAME}/bronze/market_data/**/*.json"
    files = fs.glob(input_path)
    
    if not files:
        logging.warning("no json files in bronze")
        return

    logging.info(f"found {len(files)} raw files")

    # read files and make a list
    data_list = []
    for file_path in files:
        # s3fs permite abrir archivos en la nube como si fueran locales
        with fs.open(file_path, 'r') as f:
            try:
                data = json.load(f)
                data_list.append(data)
            except Exception as e:
                logging.error(f"Error: {file_path}: {e}")

    # df
    df = pd.DataFrame(data_list)
    
    # transform
    # price to float
    df['price'] = df['price'].astype(float)
    # timestamp to date
    df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
    
    cols = ['symbol', 'asset_type', 'price', 'ingestion_timestamp']
    
    for col in cols:
        if col not in df.columns:
            df[col] = None
            
    df = df[cols]
    
    
    df = df.drop_duplicates(subset=['symbol', 'ingestion_timestamp'])

    logging.info(f" DF Final: {df.shape[0]} files. saving in parquet...")

    # save in silver
    output_path = f"s3://{BUCKET_NAME}/silver/market_data.parquet"
    
    storage_options = {'key': key, 'secret': secret}
    
    df.to_parquet(output_path, index=False, storage_options=storage_options)
    
    logging.info(f"Saved in: {output_path}")