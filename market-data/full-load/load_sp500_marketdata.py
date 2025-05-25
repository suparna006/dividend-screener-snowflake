from snowflake.snowpark import Session
import pandas as pd
import requests
import time
from datetime import datetime ,timezone
import yfinance as yf
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import os
from io import StringIO

# ------------------------
# 1. Generate RSA Key Pair (if not exists)
# ------------------------
def generate_rsa_keys():
    if not os.path.exists("rsa_key.pem"):
        print("Generating RSA key pair...")
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        
        with open("rsa_key.pem", "wb") as f:
            f.write(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            )
        with open("rsa_key.pub", "wb") as f:
            f.write(
                private_key.public_key().public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )
            )
        print("RSA key pair created: 'rsa_key.pem' (private), 'rsa_key.pub' (public)")
    else:
        print("RSA key already exists.")

generate_rsa_keys()

# ------------------------
# 2. Load Private Key and Connect to Snowflake
# ------------------------
# Path to your private key file
key_path = "rsa_key.pem"

# Load the private key
with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None  # Add password here if you encrypted the key
    )
    
connection_parameters = {
    "account": "FSXJMQY-GLB11603",
    "user": "DATAENGINEER_USR_1",
    "private_key": private_key,
    "role": "DATAENGINEER",
    "warehouse": "finance_marketdata_dw",
    "database": "FINANCE_DB",
    "schema": "RAW_SCHEMA"
}

session = Session.builder.configs(connection_parameters).create()
print("Connected to Snowflake.")

# ------------------------
# 3. Create Table if Not Exists
# ------------------------
create_table_sql = """
CREATE TABLE IF NOT EXISTS RAW_SP500_MARKET_DATA_HIST (
    TICKER STRING,
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    PRICE FLOAT,
    VOLUME NUMBER,
    SECTOR STRING,
    INDUSTRY STRING ,
    LAST_UPDATED TIMESTAMP_NTZ ,
    INSERTED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP 

)
"""
session.sql(create_table_sql).collect()
print("Verified or created table: FINANCE_DB.RAW_SCHEMA.RAW_SP500_MARKET_DATA_HIST")

# ------------------------
# 4. Fetch S&P 500 Tickers from Slickcharts
# ------------------------
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()


    html_content = response.text  # Assuming response.text contains the HTML
    tables = pd.read_html(StringIO(html_content))
    #tables = pd.read_html(response.text)
    df = tables[0]  # First table has the S&P 500 list

    symbols = df['Symbol'].tolist()

    return symbols

tickers = get_sp500_tickers()
#tickers = tickers[:200]
print(f"Found {len(tickers)} tickers from SP500.")

# ------------------------
# 5. Fetch Market Data via yfinance
# ------------------------

def fetch_market_data_in_batches(tickers, batch_size=100, delay_between_batches=30):
    all_data = []
    now = datetime.now(timezone.utc)
    
    
    start_date = (now - pd.DateOffset(years=5)).strftime('%Y-%m-%d')  # Calculate start date
    end_date = now.strftime('%Y-%m-%d')

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {len(batch)} tickers")

        batch_data = []
        for ticker in batch:
            try:
                stock = yf.Ticker(ticker)
                historical_data = stock.history(period="5y")  # Get last 5 years of data
                info = stock.info
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')

                if historical_data.empty:
                    print(f"Skipping {ticker}: No valid historical data.")
                    continue

                for index, row in historical_data.iterrows():
                    batch_data.append({
                        'TICKER': ticker.upper(),
                        'DATE': index.strftime('%Y-%m-%d'),
                        'PRICE': row['Close'],
                        'VOLUME': row['Volume'],
                        'OPEN': row['Open'],
                        'HIGH': row['High'],
                        'LOW': row['Low'],
                        'LAST_UPDATED': now,
                        'SECTOR': sector,
                        'INDUSTRY': industry
                    })
            except Exception as e:
                print(f"Error with {ticker}: {e}")

        # Convert to DataFrame and write to Snowflake
        if batch_data:
            df_batch = pd.DataFrame(batch_data)
            df_batch['LAST_UPDATED'] = pd.to_datetime(df_batch['LAST_UPDATED'])

            session.write_pandas(df_batch, "RAW_SP500_MARKET_DATA_HIST", overwrite=False, use_logical_type=True)
            print(f"Uploaded batch {i//batch_size + 1} to Snowflake")

        all_data.extend(batch_data)
        time.sleep(delay_between_batches)  # Pause to respect rate limits

    return pd.DataFrame(all_data)

df = fetch_market_data_in_batches(tickers, batch_size=50, delay_between_batches=30)
print(f"Retrieved market data for {len(df)} companies over the past 5 years.")

