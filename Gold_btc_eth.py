import pandas as pd
import numpy as np
import pyodbc
from datetime import date, timedelta
import Silver_Btc_Eth

# ---------- CONFIG ----------
SERVER   = r"KRISHNA\KVSTG"
DATABASE = "Crypto_Analytics"
DRIVER   = "ODBC Driver 17 for SQL Server"

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

# ---------- LOAD SILVER TABLES ----------
def load_silver(table_name, suffix):
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
    df = pd.read_sql(sql, conn)
    df['PriceDate'] = pd.to_datetime(df['PriceDate'])
    return df

btc_silver = load_silver("raw_btc_prices_sil", "BTC")
eth_silver = load_silver("raw_eth_prices_sil", "ETH")

# ---------- FEATURE ENGINEERING (GOLD LAYER) ----------
def engineer_features(df, suffix):
    df = df.copy().sort_values("PriceDate")
    price = f"Close_{suffix}"
    vol   = f"Volume_{suffix}"
    
    # 1) DailyReturn
    df["DailyReturn"] = df[price].pct_change()
    
    # 2) LogReturn: fill first NaN with DailyReturn
    log_col = f"LogReturn_{suffix}"
    df[log_col] = np.log(df[price] / df[price].shift(1))
    if pd.isna(df.at[df.index[0], log_col]):
        df.at[df.index[0], log_col] = df.at[df.index[0], "DailyReturn"] if pd.notna(df.at[df.index[0], "DailyReturn"]) else 0.0

    # 3) Rolling features with partial‐window and population std
    for w in (7, 30, 90):
        df[f"SMA{w}_{suffix}"] = df[price].rolling(window=w, min_periods=1).mean()
        df[f"Vol{w}_{suffix}"] = df["DailyReturn"].rolling(window=w, min_periods=1).std(ddof=0)
    # 30-day avg volume
    df[f"VolAvg30_{suffix}"] = df[vol].rolling(window=30, min_periods=1).mean()
    
    # 4) CumulativeReturn & Drawdown
    df[f"CumulativeReturn_{suffix}"] = (1 + df["DailyReturn"]).cumprod() - 1
    running_max = df[price].cummax()
    df[f"Drawdown_{suffix}"] = (df[price] - running_max) / running_max
    
    # 5) Time flags
    df["Year"]      = df["PriceDate"].dt.year
    df["Month"]     = df["PriceDate"].dt.month
    df["DayOfWeek"] = df["PriceDate"].dt.dayofweek
    
    # 6) Drop only the very first row (DailyReturn NaN on that row)
    return df.iloc[1:].reset_index(drop=True)

btc_gold = engineer_features(btc_silver, "BTC")
eth_gold = engineer_features(eth_silver, "ETH")

# ---------- INSERT INTO GOLD TABLES ----------
def refresh_gold(df_gold, table_name, suffix):
    df = df_gold.copy()
    vol    = f"Volume_{suffix}"
    vol_m  = f"{vol}_Millions"
    df[vol_m] = df[vol] / 1e6

    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Truncated {table_name}")

    cols = [
        "PriceDate",
        f"Open_{suffix}", f"High_{suffix}", f"Low_{suffix}", f"Close_{suffix}",
        vol, "DailyReturn",
        f"LogReturn_{suffix}",
        vol_m, "Coin",
        f"CumulativeReturn_{suffix}", f"Drawdown_{suffix}",
        "Year", "Month", "DayOfWeek", f"VolAvg30_{suffix}"
    ] + [f"SMA{w}_{suffix}" for w in (7,30,90)] + [f"Vol{w}_{suffix}" for w in (7,30,90)]

    placeholders = ", ".join("?" for _ in cols)
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

    rows = []
    for _, row in df.iterrows():
        vals = []
        for col in cols:
            v = row[col]
            if pd.isna(v) or (isinstance(v, float) and np.isinf(v)):
                vals.append(None)
            else:
                if col == "PriceDate":
                    vals.append(v.date())
                elif col == "Coin":
                    vals.append(str(v))
                elif col in ("Year","Month","DayOfWeek"):
                    vals.append(int(v))
                elif col == vol:
                    vals.append(int(v))
                else:
                    vals.append(float(v))
        rows.append(tuple(vals))

    cursor.fast_executemany = True
    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f" Inserted {len(rows):,} rows into {table_name}")

# Write both gold tables
refresh_gold(btc_gold, "gold_btc_prices", "BTC")
refresh_gold(eth_gold, "gold_eth_prices", "ETH")

# ---------- CLEAN UP ----------
conn.close()
