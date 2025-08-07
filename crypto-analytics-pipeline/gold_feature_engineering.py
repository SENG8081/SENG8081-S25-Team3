import pandas as pd                                       # pandas for DataFrame operations
import numpy as np                                        # NumPy for numerical functions
import pyodbc                                             # pyodbc for ODBC database connections

# ─── IMPORT UPSTREAM LAYERS ────────────────────────────────────────────────
import silver_clean_transform
# ---------- CONFIG ----------
SERVER   = r"KRISHNA\KVSTG"                               # SQL Server instance
DATABASE = "Crypto_Analytics"                             # target database
DRIVER   = "ODBC Driver 17 for SQL Server"                # ODBC driver name

# ---------- DB CONNECTION SETUP ----------
def get_db_connection():
    """
    Create and return a new pyodbc connection and cursor.
    """
    conn = pyodbc.connect(
        f"Driver={{{DRIVER}}};"                            # specify ODBC driver
        f"Server={SERVER};"                                # specify server
        f"Database={DATABASE};"                            # specify database
        "Trusted_Connection=yes;"                          # use Windows auth
        "Encrypt=yes;"                                     # encrypt connection
        "TrustServerCertificate=yes;"                      # trust the server cert
    )
    cur = conn.cursor()                                   # create cursor for SQL
    cur.fast_executemany = True                           # enable fast bulk operations
    return conn, cur

# ---------- LOAD CLEANED SILVER DATA ----------
def load_cleaned_silver_data(table_name: str, suffix: str) -> pd.DataFrame:
    """
    Load cleaned silver data for the given suffix into a pandas DataFrame.
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
    conn, _ = get_db_connection()                          # open new connection for read
    df = pd.read_sql(sql, conn, parse_dates=["PriceDate"]) # execute query, parse dates
    conn.close()                                           # close after read
    return df

# ---------- COMPUTE BASE (SILVER‐LAYER) METRICS ----------
def compute_base_metrics(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """
    Compute silver‐layer features:
      - DailyReturn
      - 7, 30, 90-day SMAs & volatilities
    """
    df = df.copy().sort_values("PriceDate")                # work on a sorted copy
    price_col = f"Close_{suffix}"                          # closing price column

    df["DailyReturn"] = df[price_col].pct_change()         # daily % change

    for window in (7, 30, 90):                             # for each lookback
        df[f"SMA{window}_{suffix}"] = df[price_col]        \
            .rolling(window).mean()                       # simple moving average
        df[f"Vol{window}_{suffix}"] = df["DailyReturn"]    \
            .rolling(window).std()                        # rolling volatility

    return df.dropna().reset_index(drop=True)              # drop NaNs, reset index

# ---------- COMPUTE ENHANCED (GOLD‐LAYER) METRICS ----------
def compute_enhanced_metrics(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """
    Compute gold‐layer features:
      - LogReturn
      - 30-day avg volume
      - CumulativeReturn & Drawdown
      - Time flags (Year, Month, DayOfWeek)
    Assumes compute_base_metrics has been applied.
    """
    df = df.copy().sort_values("PriceDate")
    price_col = f"Close_{suffix}"
    vol_col   = f"Volume_{suffix}"

    # log return
    log_col = f"LogReturn_{suffix}"
    df[log_col] = np.log(df[price_col] / df[price_col].shift(1))
    if pd.isna(df.at[0, log_col]):                        # fill first if NaN
        df.at[0, log_col] = df.at[0, "DailyReturn"]

    # 30-day avg volume
    df[f"VolAvg30_{suffix}"] = df[vol_col]                 \
        .rolling(window=30, min_periods=1).mean()

    # cumulative return
    df[f"CumulativeReturn_{suffix}"] = (1 + df["DailyReturn"]) \
        .cumprod() - 1

    # drawdown
    running_max = df[price_col].cummax()                   # highest so far
    df[f"Drawdown_{suffix}"] = (df[price_col] - running_max) \
        / running_max

    # time flags
    df["Year"]      = df["PriceDate"].dt.year
    df["Month"]     = df["PriceDate"].dt.month
    df["DayOfWeek"] = df["PriceDate"].dt.dayofweek

    return df.iloc[1:].reset_index(drop=True)              # drop initial NaN row

# ---------- UPSERT INTO GOLD TABLE ----------
def upsert_gold_table(df: pd.DataFrame, table_name: str, suffix: str):
    """
    Truncate & insert the fully‐engineered gold‐layer DataFrame into SQL.
    """
    df[f"Volume_{suffix}_Millions"] = df[f"Volume_{suffix}"] / 1e6  # compute millions

    conn, cursor = get_db_connection()                       # open new connection
    cursor.execute(f"TRUNCATE TABLE {table_name}")           # clear old data
    print(f"🗑️  Truncated {table_name}")

    # build insert statement
    cols = [
        "PriceDate",
        f"Open_{suffix}", f"High_{suffix}", f"Low_{suffix}", f"Close_{suffix}",
        f"Volume_{suffix}", "DailyReturn", f"LogReturn_{suffix}",
        f"Volume_{suffix}_Millions", "Coin",
        f"CumulativeReturn_{suffix}", f"Drawdown_{suffix}",
        "Year", "Month", "DayOfWeek"
    ] + [f"SMA{w}_{suffix}" for w in (7, 30, 90)] \
      + [f"Vol{w}_{suffix}" for w in (7, 30, 90)] \
      + [f"VolAvg30_{suffix}"]

    placeholders = ", ".join("?" for _ in cols)
    insert_sql   = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

    # prepare and insert rows
    rows = []
    for _, row in df.iterrows():
        vals = []
        for col in cols:
            v = row[col]
            if pd.isna(v) or (isinstance(v, float) and np.isinf(v)):
                vals.append(None)
            elif col == "PriceDate":
                vals.append(v.date())                   # SQL DATE
            elif col in ("Year","Month","DayOfWeek"):
                vals.append(int(v))
            elif col.startswith("Volume_") and not col.endswith("_Millions"):
                vals.append(int(v))
            else:
                vals.append(v)
        rows.append(tuple(vals))

    cursor.fast_executemany = True
    cursor.executemany(insert_sql, rows)                  # bulk insert
    conn.commit()                                          # commit
    print(f"✅  Inserted {len(rows):,} rows into {table_name}")
    cursor.close()                                         # cleanup
    conn.close()

# ---------- MAIN PIPELINE ----------
def main():
    # 1) run bronze → silver
    silver_clean_transform.main()                                         # ensures silver tables are up-to-date

    # 2) load cleaned silver
    btc_silver = load_cleaned_silver_data("raw_btc_prices_sil", "BTC")
    eth_silver = load_cleaned_silver_data("raw_eth_prices_sil", "ETH")

    # 3) compute base metrics
    btc_base = compute_base_metrics(btc_silver, "BTC")
    eth_base = compute_base_metrics(eth_silver, "ETH")

    # 4) compute enhanced metrics
    btc_gold = compute_enhanced_metrics(btc_base, "BTC")
    eth_gold = compute_enhanced_metrics(eth_base, "ETH")

    # 5) upsert into gold tables
    upsert_gold_table(btc_gold, "gold_btc_prices", "BTC")
    upsert_gold_table(eth_gold, "gold_eth_prices", "ETH")

if __name__ == "__main__":
    main()
