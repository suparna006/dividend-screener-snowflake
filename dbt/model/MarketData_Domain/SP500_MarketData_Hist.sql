{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    transient=false
    )
}}

with src_marketdata as
(
    select 
      TICKER
    , MARKET_DATE
    , OPEN
    , HIGH
    , LOW
    , CLOSE
    , VOLUME
    , INSERTED_DATE as Src_Inserted_Date
    , to_date(getdate()) as INSERTED_DATE
    from 
     {{ ref('staging_market_data') }}
     )

SELECT * FROM src_marketdata
where Src_Inserted_Date is not null

{% if is_incremental() %}
  AND Src_Inserted_Date not in  (select INSERTED_DATE from {{ this }})
{% endif %}
