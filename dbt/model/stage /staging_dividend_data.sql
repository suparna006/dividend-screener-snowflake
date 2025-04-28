WITH src_raw_dividends AS (
    SELECT
        rd.ticker, rd.dividend ,rd.dividend_date, rd.inserted_at
    FROM
     finance_db.raw_schema.raw_dividends rd inner join 
     finance_db.references_schema.tickers_inscope ti
     on rd.ticker=ti.tickers
)
SELECT
    TICKER, DIVIDEND, DIVIDEND_DATE, INSERTED_AT
FROM
    src_raw_dividends
