"""
engine.py — Orchestrates data fetching, calendar lookup, and per-day strategy execution.
"""

import datetime
import pandas as pd

from data_fetcher  import fetch_5min
from news_calendar import build_calendar, get_pair
from strategy      import run_day

# --- Account config ---
ACCOUNT_SIZE  = 5000.00   # Starting account in dollars
EURUSD_UNITS  = 200_000   # Units traded per EUR/USD position
USDJPY_UNITS  = 250_000   # Units traded per USD/JPY position


def backtest(start_date: str, end_date: str) -> list[dict]:
    """
    Run the full backtest from start_date to end_date (both "YYYY-MM-DD").

    Returns a list of trade result dicts, one per trade taken.
    """

    print("\n=== BACKTEST SETUP ===")
    print(f"  Range:   {start_date} → {end_date}")
    print(f"  Account: ${ACCOUNT_SIZE:,.2f}")
    print()

    # ------------------------------------------------------------------
    # 1. Build holiday + red-folder calendar for the whole range
    # ------------------------------------------------------------------
    print("[ 1/3 ] Loading news calendar ...")
    calendar = build_calendar(start_date, end_date)
    print()

    # ------------------------------------------------------------------
    # 2. Fetch price data for both pairs
    # ------------------------------------------------------------------
    print("[ 2/3 ] Fetching price data ...")
    eurusd_df = fetch_5min("EURUSD", start_date, end_date)
    usdjpy_df = fetch_5min("USDJPY", start_date, end_date)
    print()

    # ------------------------------------------------------------------
    # 3. Split price data by calendar date for fast per-day lookup
    # ------------------------------------------------------------------
    def split_by_day(df: pd.DataFrame) -> dict:
        df = df.copy()
        df["_date"] = df["datetime"].dt.date
        return {date: group.drop(columns="_date").reset_index(drop=True)
                for date, group in df.groupby("_date")}

    eurusd_by_day = split_by_day(eurusd_df)
    usdjpy_by_day = split_by_day(usdjpy_df)

    # ------------------------------------------------------------------
    # 4. Iterate trading days
    # ------------------------------------------------------------------
    print("[ 3/3 ] Running strategy ...")

    all_dates = sorted(set(eurusd_by_day.keys()) | set(usdjpy_by_day.keys()))

    start_d = datetime.date.fromisoformat(start_date)
    end_d   = datetime.date.fromisoformat(end_date)
    all_dates = [d for d in all_dates if start_d <= d <= end_d]

    trades: list[dict] = []

    for date in all_dates:
        pair = get_pair(date, calendar)

        if pair is None:
            continue   # USD bank holiday

        day_df = eurusd_by_day.get(date) if pair == "EURUSD" else usdjpy_by_day.get(date)

        if day_df is None or day_df.empty:
            continue

        result = run_day(day_df, pair)

        if result is None:
            continue

        outcome   = result["outcome"]
        risk_dist = result["risk_dist"]

        if risk_dist == 0:
            continue

        # R:R = distance to full TP / distance to SL
        rr = round(abs(result["full_tp"] - result["entry"]) / risk_dist, 2)

        # R gained/lost on this trade
        r_result = round(outcome["profit_raw"] / risk_dist, 2)

        # Dollar P&L — formula differs by pair:
        # EUR/USD: quote is USD so P&L = units × price_movement
        # USD/JPY: quote is JPY so P&L = units × price_movement / entry_price
        if pair == "EURUSD":
            profit_dollars = round(EURUSD_UNITS * outcome["profit_raw"], 2)
        else:
            profit_dollars = round(USDJPY_UNITS * outcome["profit_raw"] / result["entry"], 2)

        trades.append({
            "date":           date,
            "pair":           pair,
            "direction":      result["direction"],
            "entry":          result["entry"],
            "stop":           result["stop"],
            "tp1":            result["tp1"],
            "full_tp":        result["full_tp"],
            "psh":            result["psh"],
            "psl":            result["psl"],
            "midpoint":       result["midpoint"],
            "entry_time":     result["entry_time"],
            "sweep":          result["sweep_direction"].upper(),
            "R:R":            f"1:{rr}",
            "r_result":       r_result,
            "profit_dollars": profit_dollars,
            "W/L":            outcome["label"],
        })

    return trades
