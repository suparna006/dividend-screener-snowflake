from snowflake.snowpark import Session
import pandas as pd
import requests
import time
from datetime import datetime, timezone
import yfinance as yf
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import os
from io import StringIO
import pandas_market_calendars as mcal  # NEW

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
key_path = "rsa_key.pem"

with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
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
CREATE TABLE IF NOT EXISTS RAW_MARKETDATA (
    TICKER STRING,
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    PRICE FLOAT,
    VOLUME NUMBER,
    SECTOR STRING,
    INDUSTRY STRING,
    LAST_UPDATED TIMESTAMP_NTZ,
    INSERTED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
"""
session.sql(create_table_sql).collect()
print("Verified or created table: FINANCE_DB.RAW_SCHEMA.RAW_MARKETDATA")

# ------------------------
# 4. Fetch S&P 500 Tickers from Wikipedia
# ------------------------
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    html_content = response.text
    tables = pd.read_html(StringIO(html_content))
    df = tables[0]
    symbols = df['Symbol'].tolist()
    return symbols

tickers = get_sp500_tickers()
print(f"Found {len(tickers)} tickers from SP500.")

# ------------------------
# 5. Determine Previous Trading Day
# ------------------------
def get_previous_trading_day():
    nyse = mcal.get_calendar('NYSE')
    now = datetime.now(timezone.utc)

    schedule = nyse.valid_days(
        start_date=(now - pd.Timedelta(days=10)).strftime('%Y-%m-%d'),
        end_date=now.strftime('%Y-%m-%d')
    )

    if not schedule.empty:
        previous_day = schedule[-2] if schedule[-1].date() == now.date() else schedule[-1]
        return previous_day.strftime('%Y-%m-%d')
    else:
        raise ValueError("No valid trading days found.")

# ------------------------
# 6. Fetch Market Data
# ------------------------
def fetch_market_data_in_batches(tickers, batch_size=100, delay_between_batches=30):
    all_data = []
    now = datetime.now(timezone.utc)

    try:
        previous_trading_day = get_previous_trading_day()
        today = now.strftime('%Y-%m-%d')
        print(f"Fetching data for: {previous_trading_day}")
    except ValueError as ve:
        print(f"Error determining trading day: {ve}")
        return pd.DataFrame()

    # ------------------------
    # Truncate the target table
    # ------------------------
    print("Truncating existing data in RAW_MARKETDATA...")
    session.sql("TRUNCATE TABLE RAW_MARKETDATA").collect()

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}: {len(batch)} tickers")

        batch_data = []
        for ticker in batch:
            try:
                stock = yf.Ticker(ticker)
                historical_data = stock.history(start=previous_trading_day, end=today)
                info = stock.info
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')

                if historical_data.empty:
                    print(f"Skipping {ticker}: No data for {previous_trading_day}.")
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

        if batch_data:
            df_batch = pd.DataFrame(batch_data)
            df_batch['LAST_UPDATED'] = pd.to_datetime(df_batch['LAST_UPDATED'])
            session.write_pandas(df_batch, "RAW_MARKETDATA", overwrite=False, use_logical_type=True)
            print(f"Uploaded batch {i // batch_size + 1} to Snowflake")

        all_data.extend(batch_data)
        time.sleep(delay_between_batches)

    return pd.DataFrame(all_data)


# ------------------------
# 7. Run
# ------------------------
df = fetch_market_data_in_batches(tickers, batch_size=50, delay_between_batches=30)
print(f"Retrieved market data for {len(df)} records for previous trading day.")
