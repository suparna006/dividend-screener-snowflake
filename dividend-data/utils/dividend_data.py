# utils/dividend_data.py

import yfinance as yf
import pandas as pd
from datetime import datetime

def get_dividend_history(ticker: str, years: int = 5, filter_date: datetime = None) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    dividends = stock.dividends

    if dividends.empty:
        return pd.DataFrame()

    now = pd.Timestamp.now(tz=dividends.index.tz)
    cutoff = now - pd.DateOffset(years=years)
    dividends = dividends[dividends.index >= cutoff]

    if filter_date:
        filter_date = pd.Timestamp(filter_date).tz_localize(dividends.index.tz)
        dividends = dividends[dividends.index.date == filter_date.date()]

    df = dividends.reset_index()
    df.columns = ['date', 'dividend']
    return df
