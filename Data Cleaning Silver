import pyodbc
import pandas as pd
import yahoo_api_btc_eth

# ---------- CONFIG ----------
SERVER = r"KRISHNA\KVSTG"
DATABASE = "Crypto_Analytics"
DRIVER = "ODBC Driver 17 for SQL Server"

# ---------- DB CONNECTION ----------
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

# ---------- LOAD BRONZE ----------
def load_bronze_eth():
    sql = """
    SELECT 
        PriceDate,
        Open_ETH,
        High_ETH,
        Low_ETH,
        Close_ETH,
        Volume_ETH,
        Coin
    FROM raw_eth_prices_bnz
    ORDER BY PriceDate
    """
    df = pd.read_sql(sql, conn)
    df["PriceDate"] = pd.to_datetime(df["PriceDate"])
    return df

def load_bronze_btc():
    sql = """
    SELECT 
        PriceDate,
        Open_BTC,
        High_BTC,
        Low_BTC,
        Close_BTC,
        Volume_BTC,
        Coin
    FROM raw_btc_prices_bnz
    ORDER BY PriceDate
    """
    df = pd.read_sql(sql, conn)
    df["PriceDate"] = pd.to_datetime(df["PriceDate"])
    return df

# ---------- SILVER CLEAN FUNCTION (ALL DAYS) ----------
def silver_clean(df):
    df = df.copy()

    # normalize column names
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_", regex=False)
    )

    # unify date column
    date_candidates = [c for c in df.columns if c in ("date", "pricedate", "price_date")]
    if not date_candidates:
        raise KeyError(f"No date-like column found; columns: {list(df.columns)}")
    df = df.rename(columns={date_candidates[0]: "date"})

    # ensure datetime and sort/dedupe
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date", "coin"])

    # DAILY reindex (fill any missing calendar days) and forward-fill
    df = df.set_index("date")
    all_days = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(all_days).ffill()

    # name index so reset gives PriceDate
    df.index.name = "pricedate"

    # detect close column
    close_cols = [c for c in df.columns if c.startswith("close_")]
    if not close_cols:
        raise KeyError("No close_ column found; columns: " + ", ".join(df.columns))
    close_col = close_cols[0]

    # compute daily return per coin (if coin exists)
    if "coin" in df.columns:
        df["dailyreturn"] = df.groupby("coin")[close_col].pct_change()
    else:
        df["dailyreturn"] = df[close_col].pct_change()

    # scale volume columns to millions
    vol_cols = [c for c in df.columns if c.startswith("volume_")]
    for v in vol_cols:
        df[f"{v}_millions"] = df[v] / 1e6

    # reset index to get pricedate column
    df = df.reset_index()

    return df

# ---------- REFRESH SILVER TABLE ----------
def refresh_silver_table(df_silver, table_name, prefix):
    df_ready = df_silver.copy()

    # expected lowercase column names from silver_clean
    price_col = f"close_{prefix.lower()}"
    open_col = f"open_{prefix.lower()}"
    high_col = f"high_{prefix.lower()}"
    low_col = f"low_{prefix.lower()}"
    volume_col = f"volume_{prefix.lower()}"
    volume_millions_col = f"{volume_col}_millions"

    # sort
    df_ready = df_ready.sort_values(["coin", "pricedate"])

    # truncate target
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"🗑️  Truncated {table_name}")

    if prefix == "ETH":
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Open_ETH, High_ETH, Low_ETH, Close_ETH, Volume_ETH, DailyReturn, Volume_ETH_Millions, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["pricedate"],
                float(row[open_col]),
                float(row[high_col]),
                float(row[low_col]),
                float(row[price_col]),
                int(row[volume_col]) if pd.notna(row[volume_col]) else None,
                None if pd.isna(row["dailyreturn"]) else float(row["dailyreturn"]),
                float(row.get(volume_millions_col, 0.0)),
                row["coin"].capitalize() if isinstance(row["coin"], str) else row["coin"],
            )
            for _, row in df_ready.iterrows()
        ]
    else:  # BTC
        insert_sql = f"""
        INSERT INTO {table_name}
            (PriceDate, Open_BTC, High_BTC, Low_BTC, Close_BTC, Volume_BTC, DailyReturn, Volume_BTC_Millions, Coin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                row["pricedate"],
                float(row[open_col]),
                float(row[high_col]),
                float(row[low_col]),
                float(row[price_col]),
                int(row[volume_col]) if pd.notna(row[volume_col]) else None,
                None if pd.isna(row["dailyreturn"]) else float(row["dailyreturn"]),
                float(row.get(volume_millions_col, 0.0)),
                row["coin"].capitalize() if isinstance(row["coin"], str) else row["coin"],
            )
            for _, row in df_ready.iterrows()
        ]

    try:
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
        conn.commit()
        print(f"Inserted {len(rows):,} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert into {table_name}: {e}")

# ---------- MAIN ----------
eth_bronze = load_bronze_eth()
btc_bronze = load_bronze_btc()

eth_silver = silver_clean(eth_bronze)
btc_silver = silver_clean(btc_bronze)

print("=== Sample after silver_clean ===")
print("ETH:", eth_silver.head(2).to_string(index=False))
print("BTC:", btc_silver.head(2).to_string(index=False))

# Push into silver tables
refresh_silver_table(eth_silver, "raw_eth_prices_sil", "ETH")
refresh_silver_table(btc_silver, "raw_btc_prices_sil", "BTC")

# cleanup
cursor.close()
conn.close()
