import yfinance as yf                          # Yahoo Finance API for downloading historical crypto data
import pandas as pd                            # pandas for DataFrame manipulation
from datetime import date, timedelta           # date and timedelta for date arithmetic
from dateutil.relativedelta import relativedelta  # relativedelta for complex date offsets
from sqlalchemy import create_engine, text      # SQLAlchemy for potential future DB use (not used below)
import urllib                                  # urllib for URL encoding (not used below)
import pyodbc                                  # pyodbc for ODBC database connections

# ─── CLEAN FUNCTION ───────────────────────────────────────────────
def clean_df(df, coinlabel, coin_suffix):
    """
    Cleans raw yf.download output:
      1) Moves index into a Date column
      2) Removes any embedded header rows
      3) Flattens MultiIndex column names and appends suffix
    """
    # 1) Move index to column named 'Date'; drop any leftover 'index'
    df = df.reset_index().rename(columns={'index': 'Date'})
    df = df.drop(columns=['index'], errors='ignore')

    # 2) Remove rows where any cell matches common header patterns
    df = df[~df.astype(str)
        .apply(lambda row: row.str.fullmatch("Ticker|BTC-USD|ETH-USD", case=False).any(), axis=1)
    ]

    # 3) Rename columns: flatten MultiIndex and append suffix to non-Date/Coin columns
    cols = []
    for col in df.columns:
        if isinstance(col, tuple):             # MultiIndex tuple case
            base = col[0]
        else:
            base = col                          # normal column name
        if base not in ['Date', 'Coin']:
            cols.append(f"{base}_{coin_suffix}")  # e.g., 'Open_BTC' or 'Close_ETH'
        else:
            cols.append(base)                   # keep 'Date' and 'Coin' as-is
    df.columns = cols

    return df

# ─── REFRESH TABLE FUNCTION ───────────────────────────────────────
def refresh_table(df_cleaned, table_name, prefix):
    """
    Truncate the target table and bulk-insert cleaned rows.
      - df_cleaned: cleaned DataFrame
      - table_name: SQL table to refresh (raw_eth_prices_bnz or raw_btc_prices_bnz)
      - prefix: 'ETH' or 'BTC' to choose columns and coin logic
    """
    # 1. Truncate existing data
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"🗑️  Truncated {table_name}")

    # 2. Prepare INSERT SQL and rows list
    if prefix == "ETH":
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Close_ETH, High_ETH, Low_ETH, Open_ETH, Volume_ETH, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["Date"],                    # date column
                float(row["Close_ETH"]),        # closing price
                float(row["High_ETH"]),         # high price
                float(row["Low_ETH"]),          # low price
                float(row["Open_ETH"]),         # open price
                int(row["Volume_ETH"]),         # volume
                row["Coin"],                    # coin label
            )
            for _, row in df_cleaned.iterrows()
        ]
    else:  # BTC case
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Close_BTC, High_BTC, Low_BTC, Open_BTC, Volume_BTC, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["Date"],                    # date column
                float(row["Close_BTC"]),        # closing price
                float(row["High_BTC"]),         # high price
                float(row["Low_BTC"]),          # low price
                float(row["Open_BTC"]),         # open price
                int(row["Volume_BTC"]),         # volume
                row["Coin"],                    # coin label
            )
            for _, row in df_cleaned.iterrows()
        ]

    # 3. Bulk insert and handle errors
    try:
        cursor.executemany(insert_sql, rows)  # execute batch insert
        conn.commit()                         # commit transaction
        print(f"✅ Inserted {len(rows):,} rows into {table_name}")
    except Exception as e:
        conn.rollback()                       # rollback on error
        print(f"❌ Failed to insert into {table_name}: {e}")

# ─── MAIN ENTRY POINT ─────────────────────────────────────────────
def main():
    # Set Date Range 
    today = date.today()                           # get today's date
    start_date = (today - relativedelta(years=7)).strftime("%Y-%m-%d")  # seven years ago, formatted
    end_date   = (today - timedelta(days=1)).strftime("%Y-%m-%d")       # yesterday, formatted

    # Download Data 
    btc_df = yf.download("BTC-USD", start=start_date, end=end_date, interval="1d")  # daily BTC data
    eth_df = yf.download("ETH-USD", start=start_date, end=end_date, interval="1d")  # daily ETH data

    # Add 'Coin' Column 
    btc_df['Coin'] = 'Bitcoin'                    # label BTC rows
    eth_df['Coin'] = 'Ethereum'                   # label ETH rows

    # Inspect initial DataFrames
    print(btc_df.info())                           # show structure, dtypes, non-null counts for BTC
    print(eth_df.describe())                       # statistical summary for ETH

    # Apply Cleaning 
    btc_df_cleaned = clean_df(btc_df, "Bitcoin", "BTC")   # clean BTC DataFrame
    eth_df_cleaned = clean_df(eth_df, "Ethereum", "ETH")  # clean ETH DataFrame

    # Save Cleaned CSVs 
    btc_df_cleaned.to_csv("btc_cleaned_final.csv", index=False)  # export BTC to CSV
    eth_df_cleaned.to_csv("eth_cleaned_final.csv", index=False)  # export ETH to CSV
    print("\n✅ Cleaned CSVs saved without index columns.")

    # Connect (Windows Auth) to SQL Server Crypto_Analytics database
    global conn, cursor
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"  # specify ODBC driver
        "Server=KRISHNA\\KVSTG;"                  # SQL Server instance
        "Database=Crypto_Analytics;"              # target database
        "Trusted_Connection=yes;"                 # use Windows authentication
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()                        # create a cursor for executing SQL
    cursor.fast_executemany = True                # enable fast executemany for bulk inserts

    # Execute refresh for both layers
    refresh_table(eth_df_cleaned, "raw_eth_prices_bnz", "ETH")  # load ETH table
    refresh_table(btc_df_cleaned, "raw_btc_prices_bnz", "BTC")  # load BTC table

    # cleanup
    cursor.close()  # close cursor
    conn.close()    # close DB connection

if __name__ == "__main__":
    main()
