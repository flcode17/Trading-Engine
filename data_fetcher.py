import os
import time
import pandas as pd
from twelvedata import TDClient

# Reads API key from environment (loaded via `source /Users/andrew/.keys`)
_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

# Twelve Data symbol names for each pair
_SYMBOLS = {
    "EURUSD": "EUR/USD",
    "USDJPY": "USD/JPY",
}

# Free tier: 8 requests/minute. We sleep between calls to stay safe.
_REQUEST_DELAY_SECONDS = 8


def fetch_5min(pair: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch 5-minute OHLC data for `pair` between start_date and end_date.

    Args:
        pair:       "EURUSD" or "USDJPY"
        start_date: "YYYY-MM-DD"
        end_date:   "YYYY-MM-DD"

    Returns:
        DataFrame with columns: datetime, open, high, low, close
        datetime is timezone-aware in America/New_York.
    """
    if not _API_KEY:
        raise EnvironmentError(
            "TWELVE_DATA_API_KEY not set. Run `source /Users/andrew/.keys` "
            "before starting the backtester."
        )

    symbol = _SYMBOLS.get(pair.upper())
    if symbol is None:
        raise ValueError(f"Unsupported pair '{pair}'. Choose EURUSD or USDJPY.")

    td = TDClient(apikey=_API_KEY)

    print(f"  Fetching {pair} 5-min data {start_date} → {end_date} ...")

    # Twelve Data returns data newest-first by default; we request oldest-first
    ts = td.time_series(
        symbol=symbol,
        interval="5min",
        start_date=start_date,
        end_date=end_date,
        timezone="America/New_York",
        order="ASC",
        outputsize=5000,   # max per call
    )

    # Twelve Data SDK returns a pandas DataFrame via .as_pandas()
    df = ts.as_pandas()

    # Respect free-tier rate limit
    time.sleep(_REQUEST_DELAY_SECONDS)

    if df is None or df.empty:
        raise RuntimeError(
            f"No data returned for {pair} ({start_date} → {end_date}). "
            "Check your API key and date range."
        )

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]

    # Rename 'datetime' column if it comes back as 'date' or similar
    if "datetime" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "datetime"})

    # Ensure datetime is tz-aware in New York time
    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize("America/New_York")
    else:
        df["datetime"] = df["datetime"].dt.tz_convert("America/New_York")

    df = df[["datetime", "open", "high", "low", "close"]].copy()
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    df = df.sort_values("datetime").reset_index(drop=True)

    # If the date range spans more than ~5000 candles, Twelve Data will truncate.
    # Warn the user so they can split the range manually.
    trading_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
    expected_candles = trading_days * 12 * 6.5  # ~78 candles/trading day
    if len(df) < expected_candles * 0.5:
        print(
            f"  WARNING: Got {len(df)} candles but expected ~{int(expected_candles)}. "
            "Your date range may exceed the 5000-candle limit per request. "
            "Consider splitting into shorter ranges (e.g. 3-month chunks)."
        )
    else:
        print(f"  Got {len(df)} candles for {pair}.")

    return df
