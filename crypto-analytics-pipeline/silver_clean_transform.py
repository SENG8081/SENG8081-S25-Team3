import pandas as pd                        # pandas for DataFrame operations
import pyodbc                              # pyodbc for DB connections
import bronze_raw_ingest                              # our Bronze layer module

# ---------- CONFIG ----------
SERVER   = r"KRISHNA\KVSTG"                # SQL Server instance
DATABASE = "Crypto_Analytics"              # target database
DRIVER   = "ODBC Driver 17 for SQL Server" # ODBC driver

# ---------- LOAD & CLEAN BRONZE DATA ----------
def load_and_clean_bronze(table_name: str, suffix: str) -> pd.DataFrame:
    """
    1) Load raw bronze data into DataFrame
    2) Drop duplicates, remove non-positive values,
       forward-fill missing, trim Coin, reset index
    """
    sql = f"""
    SELECT
      PriceDate,
      Open_{suffix},
      High_{suffix},
      Low_{suffix},
      Close_{suffix},
      Volume_{suffix},
      Coin
    FROM {table_name}
    ORDER BY PriceDate
    """
    df = pd.read_sql(sql, conn, parse_dates=["PriceDate"])

    # 1) Drop exact duplicate dates
    df = df.drop_duplicates(subset=["PriceDate"])

    # 2) Remove rows with zero or negative prices/volume
    price_cols = [f"{c}_{suffix}" for c in ("Open","High","Low","Close")]
    df = df[(df[price_cols] > 0).all(axis=1) & (df[f"Volume_{suffix}"] > 0)]

    # 3) Forward-fill then drop any remaining NaNs
    df.sort_values("PriceDate", inplace=True)
    df.fillna(method="ffill", inplace=True)
    df.dropna(inplace=True)

    # 4) Trim whitespace in Coin column
    df["Coin"] = df["Coin"].str.strip()

    # 5) Reset index
    df.reset_index(drop=True, inplace=True)
    return df

# ---------- WRITE TO SILVER TABLE ----------
def refresh_silver(df: pd.DataFrame, table_name: str, suffix: str):
    """
    Truncate and bulk-insert cleaned rows into the silver table.
    """
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"🗑️ Truncated {table_name}")

    cols = [
        "PriceDate",
        f"Open_{suffix}", f"High_{suffix}", f"Low_{suffix}", f"Close_{suffix}",
        f"Volume_{suffix}", "Coin"
    ]
    placeholders = ", ".join("?" for _ in cols)
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

    rows = [tuple(row[col] for col in cols) for _, row in df.iterrows()]
    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f"✅ Inserted {len(rows):,} rows into {table_name}")

# ---------- MAIN ENTRY POINT ----------
def main():
    # 1) Ensure Bronze layer has run and populated raw tables
    bronze_raw_ingest.main()

    # 2) Connect to DB
    global conn, cursor
    conn = pyodbc.connect(
        f"Driver={{{DRIVER}}};"
        f"Server={SERVER};"
        f"Database={DATABASE};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # 3) Load, clean, and write BTC to silver
    btc_clean = load_and_clean_bronze("raw_btc_prices_bnz", "BTC")
    refresh_silver(btc_clean, "raw_btc_prices_sil", "BTC")

    # 4) Load, clean, and write ETH to silver
    eth_clean = load_and_clean_bronze("raw_eth_prices_bnz", "ETH")
    refresh_silver(eth_clean, "raw_eth_prices_sil", "ETH")

    # 5) Cleanup
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
