import pandas as pd
import numpy as np

RISK = 1
REWARD = 2
RISK_DOLLARS = 100   # $ risk per 1R
RISK_PERCENT = 1.5   # % of account risked per 1R

# --- SESSION TIMES (all in New York / US Eastern time) ---
LONDON_SESSION_START = 3   # 3:00 AM NY time
LONDON_SESSION_END   = 8   # 8:00 AM NY time
NY_SESSION_START     = 8   # 8:00 AM NY time
NY_SESSION_END       = 13  # 1:00 PM NY time


def backtest(df_1min):
    """
    df_1min: 1-minute OHLC dataframe with column 'datetime' as string/datetime
    Returns: list of trades with Date, R:R, profit %, profit $, W/L

    Strategy logic:
      1. During London session (3am-8am NY), track the session high/low.
      2. During NY session (8am-1pm NY), scan 5min candles for:
           - Sweep of the London session high or low
           - Break of Structure (BOS)
           - FVG or IFVG confirmation
      3. Only one trade per day.
    """

    # --- Localise to New York time ---
    df_1min['datetime'] = pd.to_datetime(df_1min['datetime'])

    # If data has no timezone info, assume it is UTC and convert to NY.
    # If it's already in NY time, remove the tz_localize/tz_convert lines below
    # and just use df_1min['datetime'] directly.
    if df_1min['datetime'].dt.tz is None:
        df_1min['datetime'] = (
            df_1min['datetime']
            .dt.tz_localize('UTC')
            .dt.tz_convert('America/New_York')
        )
    else:
        df_1min['datetime'] = df_1min['datetime'].dt.tz_convert('America/New_York')

    df_1min.set_index('datetime', inplace=True)

    # Resample to 5-minute candles (preserving NY timezone)
    df_5min = df_1min.resample('5min').agg({
        'open':  'first',
        'high':  'max',
        'low':   'min',
        'close': 'last'
    }).dropna().reset_index()

    trades = []
    last_trade_day = None

    # --- Pre-compute London session high/low per calendar day ---
    # London session = 3am to 8am NY time
    london_mask = (
        (df_5min['datetime'].dt.hour >= LONDON_SESSION_START) &
        (df_5min['datetime'].dt.hour <  LONDON_SESSION_END)
    )
    london_candles = df_5min[london_mask].copy()
    london_candles['date'] = london_candles['datetime'].dt.date

    london_levels = (
        london_candles
        .groupby('date')
        .agg(london_high=('high', 'max'), london_low=('low', 'min'))
        .reset_index()
    )
    # Build a quick lookup dict: date -> (london_high, london_low)
    london_map = {
        row['date']: (row['london_high'], row['london_low'])
        for _, row in london_levels.iterrows()
    }

    # --- Scan NY session candles for setups ---
    ny_mask = (
        (df_5min['datetime'].dt.hour >= NY_SESSION_START) &
        (df_5min['datetime'].dt.hour <  NY_SESSION_END)
    )
    df_ny = df_5min[ny_mask].reset_index(drop=True)

    for i in range(3, len(df_ny)):
        current_day = df_ny['datetime'][i].date()

        # Only one trade per day
        if last_trade_day == current_day:
            continue

        # We need London levels for today
        if current_day not in london_map:
            continue  # no London data for this day — skip

        london_high, london_low = london_map[current_day]

        # --- SWEEP of London session high or low ---
        # A sweep means price wicked beyond the level but we look for
        # previous candle(s) to have pushed through and current/prior
        # pulled back — here we check if any of the last 3 candles
        # exceeded the London level (wick sweep).
        swept_high = any(df_ny['high'][i - k] > london_high for k in range(1, 4))
        swept_low  = any(df_ny['low'][i - k]  < london_low  for k in range(1, 4))
        sweep = swept_high or swept_low

        if not sweep:
            continue

        # --- BOS (Break of Structure) ---
        bos_bull = df_ny['close'][i] > df_ny['high'][i-1]   # bullish BOS
        bos_bear = df_ny['close'][i] < df_ny['low'][i-1]    # bearish BOS
        bos = bos_bull or bos_bear

        if not bos:
            continue

        # --- FVG / IFVG ---
        fvg_up   = df_ny['low'][i]  > df_ny['high'][i-2]
        fvg_down = df_ny['high'][i] < df_ny['low'][i-2]
        fvg = fvg_up or fvg_down

        ifvg_up   = df_ny['low'][i]  > df_ny['low'][i-1]  and df_ny['high'][i] < df_ny['high'][i-1]
        ifvg_down = df_ny['high'][i] < df_ny['high'][i-1] and df_ny['low'][i]  > df_ny['low'][i-1]
        ifvg = ifvg_up or ifvg_down

        if not (fvg or ifvg):
            continue

        # --- ENTRY ---
        entry = df_ny['close'][i]
        is_long = entry > df_ny['open'][i]
        stop    = df_ny['low'][i]  if is_long else df_ny['high'][i]
        target  = (entry + (entry - stop) * REWARD) if is_long else (entry - (stop - entry) * REWARD)

        result = simulate_trade(df_ny, i, stop, target)

        rr             = abs(target - entry) / abs(entry - stop)
        profit_percent = round(result * RISK_PERCENT, 2)
        profit_dollars = round(result * RISK_DOLLARS, 2)

        trades.append({
            'date':           df_ny['datetime'][i].date(),
            'R:R':            f"1:{round(rr,2)} ({RISK_PERCENT:.2f}% risk)",
            'profit_R':       result,
            'profit_percent': profit_percent,
            'profit_dollars': profit_dollars,
            'W/L':            'W' if result > 0 else 'L' if result < 0 else 'BE'
        })

        last_trade_day = current_day

    return trades


def simulate_trade(df, entry_idx, stop, target):
    """
    Walk forward candle by candle and see
    whether stop or target gets hit first.
    Returns: +REWARD, -RISK, or 0 (unresolved)
    """
    for j in range(entry_idx + 1, len(df)):
        if stop < target:  # long trade
            if df['low'][j] <= stop:
                return -RISK
            if df['high'][j] >= target:
                return REWARD
        else:  # short trade
            if df['high'][j] >= stop:
                return -RISK
            if df['low'][j] <= target:
                return REWARD
    return 0  # unresolved