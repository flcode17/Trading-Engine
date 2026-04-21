"""
news_calendar.py — Determines which pair to trade (or skip) for each date.

Rules:
  1. USD bank holiday                   → no trade (None)
  2. EUR bank holiday                   → trade USD/JPY
  3. Red folder USD or EUR news day     → trade USD/JPY
  4. Normal day                         → trade EUR/USD

USD holidays : US federal holidays via the `holidays` library.
EUR holidays : ECB TARGET calendar (computed in memory).
Red folder   : Investing.com economic calendar POST endpoint.
"""

import datetime
import requests
from bs4 import BeautifulSoup
import holidays as hol


# ---------------------------------------------------------------------------
# ECB TARGET holiday calculator
# ---------------------------------------------------------------------------

def _easter(year: int) -> datetime.date:
    """Return Easter Sunday for a given year (Gregorian algorithm)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day   = ((h + l - 7 * m + 114) % 31) + 1
    return datetime.date(year, month, day)


def _ecb_target_holidays(year: int) -> set[datetime.date]:
    easter = _easter(year)
    return {
        datetime.date(year, 1,  1),
        easter - datetime.timedelta(days=2),   # Good Friday
        easter + datetime.timedelta(days=1),   # Easter Monday
        datetime.date(year, 5,  1),
        datetime.date(year, 12, 25),
        datetime.date(year, 12, 26),
    }


# ---------------------------------------------------------------------------
# USD bank holidays
# ---------------------------------------------------------------------------

def _usd_holidays(year: int) -> set[datetime.date]:
    return set(hol.country_holidays("US", years=year).keys())


# ---------------------------------------------------------------------------
# Investing.com red folder fetcher
# ---------------------------------------------------------------------------

_INVESTING_URL = "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Referer":          "https://www.investing.com/economic-calendar/",
    "Content-Type":     "application/x-www-form-urlencoded",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Origin":           "https://www.investing.com",
}


def _fetch_red_folder_investing(
    start: datetime.date, end: datetime.date
) -> set[datetime.date]:
    """
    Fetch high-impact USD and EUR events from Investing.com for the given
    date range in a single POST request.

    Returns a set of dates that have at least one red folder event.
    """
    payload = {
        "country[]":     ["5", "72"],   # 5 = USD, 72 = EUR
        "importance[]":  ["3"],          # 3 = high impact only
        "dateFrom":      start.strftime("%Y-%m-%d"),
        "dateTo":        end.strftime("%Y-%m-%d"),
        "timeZone":      "8",            # Eastern Time
        "timeFilter":    "timeRemain",
        "currentTab":    "custom",
        "submitFilters": "1",
        "limit_from":    "0",
    }

    try:
        resp = requests.post(
            _INVESTING_URL,
            headers=_HEADERS,
            data=payload,
            timeout=20,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  WARNING: Investing.com returned HTTP error: {e}")
        print("  Defaulting to no red folder days — all days will trade EUR/USD.")
        return set()
    except Exception as e:
        print(f"  WARNING: Could not reach Investing.com: {e}")
        print("  Defaulting to no red folder days — all days will trade EUR/USD.")
        return set()

    try:
        json_data = resp.json()
    except Exception:
        print("  WARNING: Investing.com response was not valid JSON.")
        print("  This usually means the request was blocked (captcha/bot detection).")
        print("  Defaulting to no red folder days — all days will trade EUR/USD.")
        return set()

    html_fragment = json_data.get("data", "")
    if not html_fragment:
        print("  WARNING: Investing.com returned empty data.")
        print("  Defaulting to no red folder days — all days will trade EUR/USD.")
        return set()

    # Parse the HTML rows embedded in the JSON response
    soup = BeautifulSoup(html_fragment, "html.parser")
    red_dates: set[datetime.date] = set()

    # Investing.com date format: "2025/02/03 06:00:00" — slashes, not dashes
    for row in soup.select("tr.js-event-item"):
        attr = row.get("data-event-datetime", "")
        if not attr:
            continue
        try:
            # Normalise slashes to dashes then take the date portion
            event_date = datetime.date.fromisoformat(attr[:10].replace("/", "-"))
        except ValueError:
            continue

        if start <= event_date <= end:
            red_dates.add(event_date)

    print(f"  Red folder days found: {len(red_dates)}")
    return red_dates


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def build_calendar(start_date: str, end_date: str) -> dict:
    """
    Build a calendar dict for the full backtest range.

    Returns:
        {
            "usd_holidays": set[date],
            "eur_holidays": set[date],
            "red_folder":   set[date],
        }
    """
    start = datetime.date.fromisoformat(start_date)
    end   = datetime.date.fromisoformat(end_date)
    years = range(start.year, end.year + 1)

    usd_hols: set[datetime.date] = set()
    for y in years:
        for d in _usd_holidays(y):
            if start <= d <= end:
                usd_hols.add(d)

    eur_hols: set[datetime.date] = set()
    for y in years:
        for d in _ecb_target_holidays(y):
            if start <= d <= end:
                eur_hols.add(d)

    print(f"  USD bank holidays in range: {len(usd_hols)}")
    print(f"  EUR bank holidays in range: {len(eur_hols)}")
    print(f"  Fetching red folder events from Investing.com ...")

    red_folder = _fetch_red_folder_investing(start, end)

    return {
        "usd_holidays": usd_hols,
        "eur_holidays": eur_hols,
        "red_folder":   red_folder,
    }


def get_pair(date: datetime.date, calendar: dict) -> str | None:
    """
    Return the trading pair for a given date, or None if no trade.

      1. USD bank holiday                → None
      2. EUR bank holiday or red folder  → "USDJPY"
      3. Normal day                      → "EURUSD"
    """
    if date in calendar["usd_holidays"]:
        return None

    if date in calendar["eur_holidays"] or date in calendar["red_folder"]:
        return "USDJPY"

    return "EURUSD"
