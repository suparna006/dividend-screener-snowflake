{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    transient=false
    )
}}

with src_dividenddata as
(
    select 
        TICKER
        , DIVIDEND
        , DIVIDEND_DATE
        , INSERTED_AT as Src_Inserted_Date
        , to_date(getdate()) as INSERTED_DATE
    from 
        {{ ref('staging_dividend_data') }}
     )

SELECT * FROM src_dividenddata
where Src_Inserted_Date is not null

{% if is_incremental() %}
  AND Src_Inserted_Date not in  (select INSERTED_DATE from {{ this }})
{% endif %}
