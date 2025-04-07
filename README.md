# dividend-screener-snowflake
Dividend Data to Use data as a Product

dividend-screener-snowflake/
├── README.md
├── requirements.txt
├── .env                         # For storing Snowflake creds (not pushed)
├── main.py                      # Main script: fetch + load to Snowflake
├── utils/
│   └── dividend_data.py         # yfinance fetcher
├── sql/
│   ├── create_table.sql         # Snowflake table DDL
│   ├── create_view.sql          # Curated data view
│   └── create_share.sql         # Share setup SQL
└── config/
    └── snowflake_config.py      # Config loader from .env
