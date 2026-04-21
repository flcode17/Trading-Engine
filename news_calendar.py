import os
import datetime
import finnhub
import holidays as hol

_FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")


# ---------------------------------------------------------------------------
# USD Bank Holidays
# ---------------------------------------------------------------------------

def is_usd_bank_holiday(date: datetime.date) -> bool:
    """Return True if `date` is a USD bank holiday (US Federal Reserve calendar)."""
    us_holidays = hol.country_holidays("US", years=date.year)
    return date in us_holidays


# ---------------------------------------------------------------------------
# Red Folder (High-Impact USD News) Detection via Finnhub
# ---------------------------------------------------------------------------

def _fetch_red_folder_dates(start: datetime.date, end: datetime.date) -> set[datetime.date]:
    """
    Query Finnhub economic calendar for high-impact USD events between
    start and end (inclusive). Returns a set of dates that have red folder news.
    """
    if not _FINNHUB_KEY:
        raise EnvironmentError(
            "FINNHUB_API_KEY not set. Run `source /Users/andrew/.keys` "
            "before starting the backtester."
        )

    client = finnhub.Client(api_key=_FINNHUB_KEY)

    from_str = start.strftime("%Y-%m-%d")
    to_str   = end.strftime("%Y-%m-%d")

    print(f"  Fetching red folder news days {from_str} → {to_str} ...")

    try:
        data = client.economic_calendar(**{"from": from_str, "to": to_str})
    except Exception as e:
        print(f"  WARNING: Could not fetch Finnhub economic calendar: {e}")
        print("  Defaulting to NO red folder days (all days will trade EUR/USD).")
        return set()

    red_dates: set[datetime.date] = set()

    if not data or "economicCalendar" not in data:
        return red_dates

    for event in data["economicCalendar"]:
        # Filter: USD events only, high impact only
        if event.get("country", "").upper() != "US":
            continue
        if event.get("impact", "").lower() != "high":
            continue

        try:
            event_date = datetime.date.fromisoformat(event["time"][:10])
        except (KeyError, ValueError):
            continue

        if start <= event_date <= end:
            red_dates.add(event_date)

    print(f"  Found {len(red_dates)} red folder day(s) in range.")
    return red_dates


def build_calendar(start_date: str, end_date: str) -> dict:
    """
    Build a calendar dict for the backtest range.

    Returns:
        {
            "holidays":   set of datetime.date  (USD bank holidays),
            "red_folder": set of datetime.date  (high-impact USD news days),
        }

    Usage:
        cal = build_calendar("2024-01-01", "2024-06-30")
        if cal["holidays"] or date in cal["red_folder"]: ...
    """
    start = datetime.date.fromisoformat(start_date)
    end   = datetime.date.fromisoformat(end_date)

    # Collect all holiday dates in range
    years = range(start.year, end.year + 1)
    all_holidays: set[datetime.date] = set()
    for y in years:
        us_h = hol.country_holidays("US", years=y)
        for d in us_h:
            if start <= d <= end:
                all_holidays.add(d)

    red_folder_dates = _fetch_red_folder_dates(start, end)

    print(f"  USD bank holidays in range: {len(all_holidays)}")

    return {
        "holidays":   all_holidays,
        "red_folder": red_folder_dates,
    }


def get_pair(date: datetime.date, calendar: dict) -> str | None:
    """
    Return the trading pair for a given date, or None if we should not trade.

    Rules:
      - USD bank holiday → None (no trade)
      - Red folder day   → "USDJPY"
      - Otherwise        → "EURUSD"
    """
    if date in calendar["holidays"]:
        return None   # no trading on bank holidays
    if date in calendar["red_folder"]:
        return "USDJPY"
    return "EURUSD"
