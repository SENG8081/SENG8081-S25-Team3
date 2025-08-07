import warnings                             # to control warning messages
import pandas as pd                        # pandas for DataFrame operations
import pyodbc                               # pyodbc for ODBC database connections
from statsmodels.tsa.holtwinters import ExponentialSmoothing  # Holt–Winters forecasting model
from datetime import datetime, timedelta   # datetime and timedelta for date arithmetic
import gold_feature_engineering                        # our Gold layer module, which itself calls Silver (and Bronze) when run

# ---------- SUPPRESS WARNINGS & CONFIG ----------
warnings.filterwarnings("ignore")          # ignore deprecation and other warnings
SERVER   = r"KRISHNA\KVSTG"                # SQL Server instance name
DATABASE = "Crypto_Analytics"              # target database name
DRIVER   = "ODBC Driver 17 for SQL Server" # ODBC driver name
PLAT_TABLE = "dbo.platinum_crypto_horizon" # fully qualified forecast results table

# ─── FORECAST HORIZONS DEFINITION (up to 10 years) ────────────────
HORIZONS = {
    "Tomorrow":   1,
    "7 Days":     7,
    "1 Month":   30,
    "3 Months":  90,
    "6 Months": 180,
    "1 Year":   365,
    "2 Years":  365 * 2,
    "3 Years":  365 * 3,
    "4 Years":  365 * 4,
    "5 Years":  365 * 5,
    "7 Years":  365 * 7,
    "8 Years":  365 * 8,
    "9 Years":  365 * 9,
    "10 Years": 365 * 10,
}

# HELPER: LOAD GOLD DATA FOR FORECASTING 
def load_gold_since_2018(coin: str) -> pd.DataFrame:
    """
    Load daily closing prices since 2018-01-01 for the given coin.
    Returns a DataFrame with columns 'ds' (date) and 'y' (price).
    """
    yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()  # yesterday's date
    tbl = f"gold_{coin.lower()}_prices"  # table name for this coin
    sql = f"""
      SELECT PriceDate AS ds, Close_{coin} AS y
      FROM dbo.{tbl}
      WHERE PriceDate >= '2018-01-01' AND PriceDate <= '{yesterday}'
      ORDER BY PriceDate
    """
    conn = pyodbc.connect(              # open a new DB connection
        f"Driver={{{DRIVER}}};"
        f"Server={SERVER};"
        f"Database={DATABASE};"
        "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )
    df = pd.read_sql(sql, conn, parse_dates=["ds"])  # execute and parse 'ds' as datetime
    conn.close()                        # close the connection
    return df                          # return the loaded DataFrame

#  HOLT’S LINEAR FORECAST FUNCTION 
def forecast_holt(df: pd.DataFrame, coin: str):
    """
    Fit a Holt’s linear trend model and forecast for each horizon.
    Returns a list of tuples for DB insertion.
    """
    ts = (
        df.set_index("ds")["y"]         # select the 'y' series indexed by 'ds'
          .asfreq("D")                  # ensure daily frequency
          .ffill()                      # forward-fill missing days
    )
    model = ExponentialSmoothing(ts, trend="add", seasonal=None).fit(optimized=True)
    last_price = ts.iloc[-1]           # last observed price
    last_date  = ts.index[-1]          # last date in the series

    rows = []                          # collect forecast tuples here
    for label, days in HORIZONS.items():
        fc_val = model.forecast(days)[-1]  # forecast 'days' ahead, take last
        target = last_date + timedelta(days=days)  # compute the target date
        ret_pct = (fc_val - last_price) / last_price * 100  # percent return
        rows.append((coin, label, target.date(), float(fc_val), float(ret_pct)))
    return rows                        # return list of (Coin, HorizonLabel, TargetDate, Forecast, ReturnPct)

#  MAIN ENTRY POINT 
def main():
    # 1) Run the entire upstream pipeline: Bronze → Silver → Gold
    gold_feature_engineering.main()

    # 2) Load gold-layer data for forecasting
    btc_df = load_gold_since_2018("BTC")
    eth_df = load_gold_since_2018("ETH")

    # 3) Generate forecasts
    btc_rows = forecast_holt(btc_df, "BTC")
    eth_rows = forecast_holt(eth_df, "ETH")
    all_rows = btc_rows + eth_rows

    # 4) Connect and upsert into the platinum table
    conn = pyodbc.connect(
        f"Driver={{{DRIVER}}};"
        f"Server={SERVER};"
        f"Database={DATABASE};"
        "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True     # enable fast bulk inserts

    cursor.execute(f"TRUNCATE TABLE {PLAT_TABLE}")  # clear existing forecasts
    insert_sql = f"""
      INSERT INTO {PLAT_TABLE}
        (Coin, HorizonLabel, TargetDate, Forecast, ReturnPct)
      VALUES (?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_sql, all_rows)  # bulk-insert forecasts
    conn.commit()                           # commit transaction
    print(f"Inserted {len(all_rows)} rows into {PLAT_TABLE}")  # confirmation

    # 5) Cleanup
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
