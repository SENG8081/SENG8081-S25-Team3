import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
import urllib
import pyodbc

# Set Date Range 
start_date = (date.today() - relativedelta(years=7)).strftime("%Y-%m-%d")
end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# Download Data 
btc_df = yf.download("BTC-USD", start=start_date, end=end_date, interval="1d")
eth_df = yf.download("ETH-USD", start=start_date, end=end_date, interval="1d")

# Add 'Coin' Column 
btc_df['Coin'] = 'Bitcoin'
eth_df['Coin'] = 'Ethereum'


print(btc_df.info())   # columns, dtypes, non-null counts
print(eth_df.describe())

# Clean Function 
def clean_df(df, coin_label, coin_suffix):
    # 1) Move index to column, drop any old index column
    df = df.reset_index().rename(columns={'index': 'Date'})
    df = df.drop(columns=['index'], errors='ignore')

    # 2) Remove rows with embedded header labels
    df = df[~df.astype(str)
        .apply(lambda row: row.str.fullmatch("Ticker|BTC-USD|ETH-USD", case=False).any(), axis=1)
    ]

    # 3) Fix column names (flatten MultiIndex if necessary)
    cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            base = col[0]
        else:
            base = col
        if base not in ['Date', 'Coin']:
            cols.append(f"{base}_{coin_suffix}")
        else:
            cols.append(base)
    df.columns = cols

    # 4) Final cleanup: drop any leftover NaNs or zeros, remove duplicates
    df = df.dropna()
    df = df[(df[f"Close_{coin_suffix}"] > 0) & (df[f"Open_{coin_suffix}"] > 0)]
    df = df.drop_duplicates(subset=['Date', 'Coin'])

    # 5) Reset the index so it no longer shows up
    df = df.reset_index(drop=True)

    # 6) Print a clean head without index
    print(f"\n {coin_label} Data AFTER Cleaning (no index column):")
    print(df.head(3).to_string(index=False))

    return df

# Apply Cleaning 
btc_df_cleaned = clean_df(btc_df, "Bitcoin", "BTC")
eth_df_cleaned = clean_df(eth_df, "Ethereum", "ETH")

# Save Cleaned CSVs (index=False ensures no index column in file) 
btc_df_cleaned.to_csv("btc_cleaned_final.csv", index=False)
eth_df_cleaned.to_csv("eth_cleaned_final.csv", index=False)
print("\n Cleaned CSVs saved without index columns.")
import pyodbc

# connect (Windows Auth) to Crypto_Analytics
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=KRISHNA\\KVSTG;"
    "Database=Crypto_Analytics;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True  # speed up executemany

def refresh_table(df_cleaned, table_name, prefix):
    # 1. Truncate existing
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"🗑️  Truncated {table_name}")

    # 2. Prepare insert SQL and rows
    if prefix == "ETH":
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Close_ETH, High_ETH, Low_ETH, Open_ETH, Volume_ETH, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["Date"],
                float(row["Close_ETH"]),
                float(row["High_ETH"]),
                float(row["Low_ETH"]),
                float(row["Open_ETH"]),
                int(row["Volume_ETH"]),
                row["Coin"],
            )
            for _, row in df_cleaned.iterrows()
        ]
    else:  # BTC
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Close_BTC, High_BTC, Low_BTC, Open_BTC, Volume_BTC, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["Date"],
                float(row["Close_BTC"]),
                float(row["High_BTC"]),
                float(row["Low_BTC"]),
                float(row["Open_BTC"]),
                int(row["Volume_BTC"]),
                row["Coin"],
            )
            for _, row in df_cleaned.iterrows()
        ]

    # 3. Bulk insert
    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"Inserted {len(rows):,} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert into {table_name}: {e}")

# Execute refresh for both
refresh_table(eth_df_cleaned, "raw_eth_prices_bnz", "ETH")
refresh_table(btc_df_cleaned, "raw_btc_prices_bnz", "BTC")

# cleanup
cursor.close()
conn.close()
