{{
  config(
     on_schema_change='fail'
    )
}}
WITH src_raw_marketdata AS (
    SELECT
        TICKER, DATE as Market_Date, OPEN, HIGH, LOW, PRICE as Close, VOLUME, rmd.INSERTED_DATE 
    FROM
     FINANCE_DB.RAW_SCHEMA.RAW_SP500_MARKET_DATA_HIST rmd inner join 
     finance_db.references_schema.tickers_inscope ti
     on rmd.ticker=ti.tickers
)
SELECT
    TICKER, Market_Date, OPEN, HIGH, LOW, Close, VOLUME, INSERTED_DATE
FROM
    src_raw_marketdata
