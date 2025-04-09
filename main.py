from config.snowflake_config import get_snowflake_connection
from utils.dividend_data import get_dividend_history
import pandas as pd

TICKERS = ["AAPL", "MSFT", "KO"]

def main():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        df = get_dividend_history(ticker)
        if df.empty:
            continue
        df['ticker'] = ticker

        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO FINANCE_DB.DIVIDENDS_SCHEMA.DIVIDENDS (date, dividend, ticker) VALUES (%s, %s, %s)",
                (row['date'], row['dividend'], row['ticker'])
            )

    print("✅ MVP data loaded.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
