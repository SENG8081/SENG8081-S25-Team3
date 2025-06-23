import pandas as pd

# Load the raw datasets from your Downloads folder
btc_df = pd.read_csv("C:/Users/danda/Downloads/btc_daily_2014_to_yesterday.csv")
eth_df = pd.read_csv("C:/Users/danda/Downloads/eth_daily_2014_to_yesterday.csv")

# Cleaning function
def clean_crypto_data(df):
    # Remove duplicate rows
    df = df.drop_duplicates()

    # Remove rows with negative prices
    price_columns = ['Close', 'High', 'Low', 'Open']
    for col in price_columns:
        df = df[df[col] >= 0]

    # Convert 'Date' column to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    return df

# Apply cleaning
btc_cleaned = clean_crypto_data(btc_df)
eth_cleaned = clean_crypto_data(eth_df)

# Save cleaned data back to CSV
btc_cleaned.to_csv("C:/Users/danda/Downloads/btc_cleaned.csv", index=False)
eth_cleaned.to_csv("C:/Users/danda/Downloads/eth_cleaned.csv", index=False)

# Display summary
print("BTC Dataset Summary:")
print(btc_cleaned.describe())
print("\nETH Dataset Summary:")
print(eth_cleaned.describe())
