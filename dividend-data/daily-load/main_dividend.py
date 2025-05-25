import pandas as pd
import boto3
import uuid
from io import BytesIO
from datetime import datetime, timedelta
import snowflake.connector
from utils.dividend_data import get_dividend_history
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import os
import sys

# -------------------
# Load Environment Variables
# -------------------
load_dotenv()  # Loads variables from .env

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
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_TICKER_TABLE"
]

def validate_env_variables():
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

validate_env_variables()

# -------------------
# Environment Setup
# -------------------
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "dividends/"
LOG_FILE_KEY = "logs/no_dividend_log.txt"

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

# -------------------
# Snowflake Connection
# -------------------
with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
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

TICKER_QUERY = f"SELECT DISTINCT Ticker FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TICKER_TABLE}"

# -------------------
# Helper Functions
# -------------------
def get_tickers_from_snowflake():
    ctx = snowflake.connector.connect(**connection_parameters)
    cs = ctx.cursor()
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

def append_log_to_s3(log_messages, s3):
    if not log_messages:
        return

    try:
        existing_log = s3.get_object(Bucket=S3_BUCKET, Key=LOG_FILE_KEY)
        log_data = existing_log['Body'].read().decode("utf-8")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_data = ""
        else:
            raise

    new_log_entries = "\n".join(log_messages)
    combined_log = log_data.strip() + "\n" + new_log_entries

    log_buffer = BytesIO()
    log_buffer.write(combined_log.encode())
    log_buffer.seek(0)

    s3.upload_fileobj(log_buffer, S3_BUCKET, LOG_FILE_KEY)
    print(f"Appended logs to S3: {LOG_FILE_KEY}")

# -------------------
# Main Function
# -------------------
def main():
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    tickers = get_tickers_from_snowflake()
    log_messages = []
    yesterday = datetime.today() - timedelta(days=1)

    for ticker in tickers:
        df = get_dividend_history(ticker, filter_date=yesterday)

        if df.empty:
            msg = f"{datetime.today().strftime('%Y-%m-%d')} - No dividend data for {ticker} on {yesterday.date()}"
            print(msg)
            log_messages.append(msg)
            continue

        df['ticker'] = ticker
        upload_to_s3(df, ticker, s3)

    append_log_to_s3(log_messages, s3)

if __name__ == "__main__":
    main()
