import yfinance as yf
import pandas as pd
from datetime import date, timedelta

# Set 10-year range
start_date = "2014-01-01"
end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# Download BTC data (10 years)
btc = yf.download("BTC-USD", start=start_date, end=end_date, interval="1d")
btc.to_csv("btc_daily_2014_to_yesterday.csv")
print("BTC 10-year daily data saved to 'btc_daily_2014_to_yesterday.csv'")

# Download ETH data (from ~2016)
eth = yf.download("ETH-USD", start=start_date, end=end_date, interval="1d")
eth.to_csv("eth_daily_2014_to_yesterday.csv")
print("ETH data saved to 'eth_daily_2014_to_yesterday.csv' (note: starts around 2016)")
