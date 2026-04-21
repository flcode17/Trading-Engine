"""
run.py — Entry point for the backtester.

HOW TO USE:
  1. Set START_DATE and END_DATE below to your desired backtest range.
  2. Make sure your API keys are loaded:
       source /Users/andrew/.keys
  3. Run:
       python run.py
"""

import os
from engine import backtest, ACCOUNT_SIZE, EURUSD_UNITS, USDJPY_UNITS

# -----------------------------------------------------------------------
# BACKTEST CONFIG — edit these before running
# -----------------------------------------------------------------------
START_DATE = "2025-02-01"   # "YYYY-MM-DD"
END_DATE   = "2025-02-28"   # "YYYY-MM-DD"
# -----------------------------------------------------------------------


def main():
    if not os.environ.get("TWELVE_DATA_API_KEY"):
        print("ERROR: TWELVE_DATA_API_KEY not found in environment.")
        print("Run:  source /Users/andrew/.keys   then try again.")
        return
    if not os.environ.get("FINNHUB_API_KEY"):
        print("ERROR: FINNHUB_API_KEY not found in environment.")
        print("Run:  source /Users/andrew/.keys   then try again.")
        return

    trades = backtest(START_DATE, END_DATE)

    print()
    print("=" * 120)
    print("TRADE LOG")
    print("=" * 120)

    if not trades:
        print("  No trades found for this period.")
        return

    header = (
        f"{'Date':<12} {'Pair':<8} {'Dir':<6} {'Sweep':<6} "
        f"{'Entry':<10} {'Stop':<10} {'TP1':<10} {'Full TP':<10} "
        f"{'PSH':<10} {'PSL':<10} {'Mid':<10} "
        f"{'R:R':<8} {'Result':>8} {'P$':>10}  W/L  Entry Time"
    )
    print(header)
    print("-" * 132)

    wins = losses = partial_wins = 0
    eurusd_trades = usdjpy_trades = 0
    total_r      = 0.0
    total_dollars = 0.0

    for r in trades:
        tp1_str    = str(r["tp1"]) if r["tp1"] is not None else "SKIP"
        result_str = f"{r['r_result']:+.2f}R"
        dollar_str = f"${r['profit_dollars']:+,.2f}"

        print(
            f"{str(r['date']):<12} "
            f"{r['pair']:<8} "
            f"{r['direction']:<6} "
            f"{r['sweep']:<6} "
            f"{r['entry']:<10} "
            f"{r['stop']:<10} "
            f"{tp1_str:<10} "
            f"{r['full_tp']:<10} "
            f"{r['psh']:<10} "
            f"{r['psl']:<10} "
            f"{r['midpoint']:<10} "
            f"{r['R:R']:<8} "
            f"{result_str:>8} "
            f"{dollar_str:>10}  "
            f"{r['W/L']:<4} "
            f"{r['entry_time']}"
        )

        total_r      += r["r_result"]
        total_dollars += r["profit_dollars"]

        if r["pair"] == "EURUSD":
            eurusd_trades += 1
        else:
            usdjpy_trades += 1

        if   r["W/L"] == "W":  wins         += 1
        elif r["W/L"] == "L":  losses       += 1
        elif r["W/L"] == "PW": partial_wins += 1

    total_trades  = len(trades)
    win_rate      = round(((wins + partial_wins) / total_trades) * 100, 1) if total_trades else 0
    final_account = round(ACCOUNT_SIZE + total_dollars, 2)

    print("-" * 132)
    print()
    print("SUMMARY")
    print(f"  Period:         {START_DATE}  →  {END_DATE}")
    print(f"  EUR/USD units:  {EURUSD_UNITS:,}  |  USD/JPY units: {USDJPY_UNITS:,}  |  Starting account: ${ACCOUNT_SIZE:,.2f}")
    print()
    print(f"  Total trades:   {total_trades}  ({wins}W / {partial_wins}PW / {losses}L)  |  Win rate: {win_rate}%")
    print(f"  EUR/USD:        {eurusd_trades} trades")
    print(f"  USD/JPY:        {usdjpy_trades} trades")
    print(f"  Total R:        {total_r:+.2f}R")
    print(f"  Total P&L:      ${total_dollars:+,.2f}")
    print(f"  Final account:  ${final_account:,.2f}")
    print()


if __name__ == "__main__":
    main()
