import os
import requests
import pandas as pd
import boto3
import logging
from datetime import datetime
from snowflake.connector import connect, ProgrammingError
from dotenv import load_dotenv
from io import StringIO
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# --- LOAD ENV ---
# Load API key & Snowflake credentials from .env file
load_dotenv()
key_path = "rsa_key.pem"

with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None  # Add password here if you encrypted the key
    )

# --- CONFIGURATION ---
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
S3_BUCKET = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')
S3_KEY = f"sentiment/sentiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

SNOWFLAKE_CFG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "private_key" : private_key
}

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FUNCTIONS ---

def get_tickers_from_snowflake():
    try:
        conn = connect(**SNOWFLAKE_CFG)
        cursor = conn.cursor()
        cursor.execute("SELECT TICKER FROM FINANCE_DB.REFERENCEData_SCHEMA.TICKERS_INSCOPE WHERE ACTIVE='TRUE'") 
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        logger.info(f"Retrieved {len(tickers)} tickers from Snowflake.")
        return tickers
    except ProgrammingError as e:
        logger.error(f"Error reading tickers from Snowflake: {e}")
        return []

# --- Updated get_sentiment_for_ticker() ---

def get_sentiment_for_ticker(ticker):
    try:
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch sentiment for {ticker}: HTTP {response.status_code}")
            return []
        data = response.json()
        return [
            {
                'ticker': ticker,
                'title': item.get('title'),
                'source': item.get('source'),
                'time_published': item.get('time_published'),
                'sentiment_date': datetime.strptime(item.get('time_published')[:8], "%Y%m%d").date() if item.get('time_published') else None,
                'overall_sentiment_score': item.get('overall_sentiment_score'),
                'overall_sentiment_label': item.get('overall_sentiment_label'),
                'relevance_score': item['ticker_sentiment'][0]['relevance_score']
                if item.get('ticker_sentiment') else None
            } for item in data.get('feed', [])
        ]
    except Exception as e:
        logger.error(f"Exception while fetching sentiment for {ticker}: {e}")
        return []



# --- Updated insert_sentiments_to_snowflake() ---

def insert_sentiments_to_snowflake(df):
    try:
        conn = connect(**SNOWFLAKE_CFG)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO RAW_SCHEMA.raw_SENTIMENTs (ticker, title, source, overall_sentiment_score, overall_sentiment_label, relevance_score, sentiment_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['ticker'],
                row['title'],
                row['source'],
                row['overall_sentiment_score'],
                row['overall_sentiment_label'],
                row['relevance_score'],
                row['sentiment_date']
            ))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Inserted {len(df)} records into Snowflake.")
    except ProgrammingError as e:
        logger.error(f"Error inserting sentiment data into Snowflake: {e}")


def upload_to_s3(df, bucket, key):
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            region_name=AWS_REGION
        )
        s3 = session.client('s3')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Uploaded sentiment data to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")

# --- MAIN ---

def main():
    tickers = get_tickers_from_snowflake()
    all_sentiments = []

    for ticker in tickers:
        sentiments = get_sentiment_for_ticker(ticker)
        if sentiments:
            all_sentiments.extend(sentiments)

    if not all_sentiments:
        logger.warning("No sentiment data retrieved.")
        return

    df = pd.DataFrame(all_sentiments)

    insert_sentiments_to_snowflake(df)
    upload_to_s3(df, S3_BUCKET, S3_KEY)

if __name__ == '__main__':
    main()
