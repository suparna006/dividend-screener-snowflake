import pandas as pd
import boto3
import uuid
from io import BytesIO
from datetime import datetime
import snowflake.connector
from utils.dividend_data import get_dividend_history
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv
import os
import sys

# -------------------
# Load Environment Variables
# -------------------
load_dotenv()  # Loads variables from .env


# -------------------
# Validate Environment Variables
# -------------------
REQUIRED_ENV_VARS = [
    "AWS_ACCESS_KEY",
    "AWS_SECRET_KEY",
    "AWS_REGION",
    "S3_BUCKET",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA"
]


def validate_env_variables():
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)  # Exit the program with error code 1

validate_env_variables()



# -------------------
# Fetch Environment Variables
# -------------------
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "dividends/"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TICKER_TABLE = os.getenv("SNOWFLAKE_TICKER_TABLE")

# ------------------------
# 1. Load Private Key and Connect to Snowflake
# ------------------------
with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None  # If your key has a password, adjust this
    )

connection_parameters = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "private_key": private_key,
    "role": SNOWFLAKE_ROLE,
    "warehouse": SNOWFLAKE_WAREHOUSE,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA
}

#TICKER_QUERY = f"SELECT Distinct Ticker FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TICKER_TABLE}"

TICKER_QUERY = f"SELECT 'AAPL' FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TICKER_TABLE}"


# ------------------------
# 2. Helper Functions
# ------------------------
def get_tickers_from_snowflake():
    ctx = snowflake.connector.connect(**connection_parameters)
    cs = ctx.cursor()
    cs.execute("SELECT CURRENT_VERSION()")
    try:
        cs.execute(TICKER_QUERY)
        tickers = [row[0] for row in cs.fetchall()]
        return tickers
    finally:
        cs.close()
        ctx.close()

def file_exists_in_s3(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def upload_to_s3(df: pd.DataFrame, ticker: str, s3):
    date_str = datetime.today().strftime("%Y-%m-%d")
    filename = f"{ticker}_dividends_{date_str}.csv"
    key = S3_PREFIX + filename

    if file_exists_in_s3(s3, S3_BUCKET, key):
        print(f"{key} already exists in S3. Skipping upload.")
        return

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3.upload_fileobj(csv_buffer, S3_BUCKET, key)
    print(f"Uploaded {key} to S3.")

# ------------------------
# 3. Main Function
# ------------------------
def main():
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    print(f"{TICKER_QUERY}")

    tickers = get_tickers_from_snowflake()

    for ticker in tickers:
        df = get_dividend_history(ticker)
        if df.empty:
            print(f"No dividend data for {ticker}, skipping.")
            continue
        df['ticker'] = ticker
        upload_to_s3(df, ticker, s3)

if __name__ == "__main__":
    main()
