import pandas as pd

# Load the tick CSV
df = pd.read_csv("data/Mar2025.csv", header=None)
df.columns = ['timestamp', 'bid', 'ask', 'volume']

# Use bid as price
df['price'] = df['bid']

# Convert timestamp to datetime
# Example timestamp: 20250302 170000461
# Split date & time
df['datetime'] = pd.to_datetime(
    df['timestamp'].astype(str).str[:8] + ' ' + df['timestamp'].astype(str).str[8:14],
    format='%Y%m%d %H%M%S'
)

# Set datetime as index
df.set_index('datetime', inplace=True)

# Resample to 1-minute candles
ohlc = df['price'].resample('1min').ohlc()  # '1min' instead of '1T'

# Reset index for CSV
ohlc.reset_index(inplace=True)

# Save to CSV for your engine
ohlc.to_csv("data/EURUSD_March2025_1min.csv", index=False)

print("Tick data converted to 1-minute OHLC candles!")