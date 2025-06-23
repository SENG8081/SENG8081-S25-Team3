import pandas as pd
import pyodbc

# Load CSVs
btc_df = pd.read_csv(r"C:\Users\danda\Downloads\cleaned Datasets\btc_cleaned.csv")
eth_df = pd.read_csv(r"C:\Users\danda\Downloads\cleaned Datasets\eth_cleaned.csv")

# Connect to SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=LOHITH_REDDY\\DLRSQL;'
    'DATABASE=CryptoDB;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Insert Bitcoin data
for _, row in btc_df.iterrows():
    cursor.execute("""
        INSERT INTO BitcoinData ([Date], [Open], [High], [Low], [Close], [Volume], [Ticker])
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        row['Date'], row['Open'], row['High'], row['Low'], row['Close'],
        row['Volume'], row['Ticker'])

# Insert Ethereum data
for _, row in eth_df.iterrows():
    cursor.execute("""
        INSERT INTO EthereumData ([Date], [Open], [High], [Low], [Close], [Volume], [Ticker])
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        row['Date'], row['Open'], row['High'], row['Low'], row['Close'],
        row['Volume'], row['Ticker'])

# Commit and close connection
conn.commit()
cursor.close()
conn.close()

print(" Data successfully inserted into SQL Server!")
