import pandas as pd
from engine import backtest

# Load CSV
df = pd.read_csv("data/EURUSD_March2025_1min.csv")

# Run backtest
results = backtest(df)

if len(results) == 0:
    print("No trades found.")
else:
    # Print individual trades
    print("Date\tR:R\tProfit %\tProfit $\tW/L")
    total_profit_percent = 0
    total_profit_dollars = 0
    wins = 0
    losses = 0

    for r in results:
        print(f"{r['date']}\t{r['R:R']}\t{r['profit_percent']}%\t${r['profit_dollars']}\t{r['W/L']}")
        total_profit_percent += r['profit_percent']
        total_profit_dollars += r['profit_dollars']
        if r['W/L'] == 'W':
            wins += 1
        elif r['W/L'] == 'L':
            losses += 1

    # Print summary
    print("\nSummary:")
    print(f"Total Profit: {round(total_profit_percent,2)}%")
    print(f"Total $ Profit: ${round(total_profit_dollars,2)}")
    print(f"W/L: {wins}W, {losses}L")