import yfinance as yf
import pandas as pd

def get_dividend_history(ticker: str, years: int = 5) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    dividends = stock.dividends

    if dividends.empty:
        return pd.DataFrame()

    # Fix: make cutoff timezone-aware
    now = pd.Timestamp.now(tz=dividends.index.tz)
    cutoff = now - pd.DateOffset(years=years)
    dividends = dividends[dividends.index >= cutoff]

    df = dividends.reset_index()
    df.columns = ['date', 'dividend']
    return df
