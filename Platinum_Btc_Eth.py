import warnings
import pandas as pd
import pyodbc
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime, timedelta
import Gold_btc_eth

# ─── SUPPRESS DEPRECATION/IMPORT WARNINGS ─────────────────────────
warnings.filterwarnings("ignore")

# ─── CONFIG ──────────────────────────────────────────────────────
SERVER   = r"KRISHNA\KVSTG"
DATABASE = "Crypto_Analytics"
DRIVER   = "ODBC Driver 17 for SQL Server"
PLAT_TABLE = "dbo.platinum_crypto_horizon"

# ─── CONNECT ─────────────────────────────────────────────────────
conn = pyodbc.connect(
    f"Driver={{{DRIVER}}};"
    f"Server={SERVER};"
    f"Database={DATABASE};"
    "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

# ─── DATE FILTER ──────────────────────────────────────────────────
yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()

# ─── LOAD GOLD DATA (2018→yesterday) ─────────────────────────────
def load_gold_since_2018(coin: str) -> pd.DataFrame:
    tbl = f"gold_{coin.lower()}_prices"
    sql = f"""
      SELECT PriceDate AS ds, Close_{coin} AS y
      FROM dbo.{tbl}
      WHERE PriceDate >= '2018-01-01' AND PriceDate <= '{yesterday}'
      ORDER BY PriceDate
    """
    df = pd.read_sql(sql, conn, parse_dates=["ds"])
    return df

btc = load_gold_since_2018("BTC")
eth = load_gold_since_2018("ETH")

# ─── FORECAST HORIZONS DEFINITION (up to 10 years) ────────────────
HORIZONS = {
    "Tomorrow":   1,
    "7 Days":     7,
    "1 Month":   30,
    "3 Months":  90,
    "6 Months": 180,
    "1 Year":   365,
    "2 Years":  365*2,
    "3 Years":  365*3,
    "4 Years":  365*4,
    "5 Years":  365*5,
    "7 Years":  365*7,
    "8 Years":  365*8,
    "9 Years":  365*9,
    "10 Years": 365*10,
}

# ─── HOLT’S LINEAR FORECAST FUNCTION ──────────────────────────────
def forecast_holt(df: pd.DataFrame, coin: str):
    ts = df.set_index("ds")["y"].asfreq('D').ffill()  # ensure daily index
    model = ExponentialSmoothing(ts, trend="add", seasonal=None).fit(optimized=True)
    last_price = ts.iloc[-1]
    last_date  = ts.index[-1]

    rows = []
    for label, days in HORIZONS.items():
        fc_val = model.forecast(days)[-1]
        target = last_date + timedelta(days=days)
        ret_pct = (fc_val - last_price) / last_price * 100
        rows.append((coin, label, target.date(), float(fc_val), float(ret_pct)))
    return rows

btc_rows = forecast_holt(btc, "BTC")
eth_rows = forecast_holt(eth, "ETH")

# ─── UPSERT INTO PLATINUM TABLE ────────────────────────────────────
cursor.execute(f"TRUNCATE TABLE {PLAT_TABLE}")
insert_sql = f"""
  INSERT INTO {PLAT_TABLE}
    (Coin, HorizonLabel, TargetDate, Forecast, ReturnPct)
  VALUES (?, ?, ?, ?, ?)
"""
all_rows = btc_rows + eth_rows
cursor.executemany(insert_sql, all_rows)
conn.commit()
print(f"Inserted {len(all_rows)} rows into {PLAT_TABLE}")

# ─── CLEAN UP ─────────────────────────────────────────────────────
conn.close()
