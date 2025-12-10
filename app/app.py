import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import os

# Config
st.set_page_config(page_title="QuantStream Portfolio", layout="wide")

# db connect
# using env vars defined in docker-compose
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = "5432"
DB_NAME = os.getenv("POSTGRES_DB", "airflow")

# URL SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_resource
def get_database_connection():
    return create_engine(DATABASE_URL)

try:
    engine = get_database_connection()
    
    st.sidebar.success("Connected to the db")
except Exception as e:
    st.error(f"Error > DB: {e}")

# UI
st.title("📊 QuantStream: My Portfolio")

# 2 columns
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("📥 Load new operation")
    
    with st.form("portfolio_form", clear_on_submit=True):
        symbol = st.text_input("Ticker (Example: BTC, AAPL)").upper()
        asset_type = st.selectbox("Asset", ["CRYPTO", "STOCK"])
        quantity = st.number_input("Quantity", min_value=0.0, format="%.6f")
        price = st.number_input("Buy Price (USD)", min_value=0.0, format="%.2f")
        
        submitted = st.form_submit_button("Save in Portfolio")
        
        if submitted and symbol and quantity > 0:
            try:
                # write in sql using pands
                new_data = pd.DataFrame([{
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "quantity": quantity,
                    "avg_buy_price": price
                }])
                
                # 'append'
                new_data.to_sql("my_portfolio", engine, if_exists="append", index=False)
                st.success(f"✅ Saved! {quantity} of {symbol}")
            except Exception as e:
                st.error(f"❌ Error : {e}")

with col2:
    st.subheader("💼 My Portfolio Holdings")
    
    # Refresh
    if st.button("Refresh table"):
        st.cache_data.clear()

    try:
        # Read postgres
        df_portfolio = pd.read_sql("SELECT * FROM my_portfolio ORDER BY created_at DESC", engine)
        
        if not df_portfolio.empty:
            st.dataframe(df_portfolio, use_container_width=True)
            
            # some metrics
            total_assets = df_portfolio['symbol'].nunique()
            st.metric("Different assets", total_assets)
        else:
            st.info("Your portfolio is empty, load something!")
            
    except Exception as e:
        st.warning("couldn't read table")