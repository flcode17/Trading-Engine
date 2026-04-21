"""
strategy.py — Per-day strategy state machine.

Flow per trading day (all times US Eastern):
  Phase 0  Build PSH / PSL from 3:00–8:00 AM candles
  Phase 1  Detect sweep of PSH or PSL (wick OR close) during NY session (8am–1pm)
  Phase 2  After sweep: track most-recent swing high/low, wait for BOS
  Phase 3  After BOS: wait for confirmation FVG or IFVG in trade direction
           (block if opposite-direction FVG exists until it is invalidated)
  Phase 4  After confirmation: wait for continuation confluence
           (second FVG in direction OR price reaches EQ with correct-direction close)
  Phase 5  Entry validation + trade execution
  Phase 6  Trade management (TP1 at midpoint, TP2 at PSH/PSL, SL at swing high/low)

Key rules encoded here:
  - No trades after 1:00 PM EST
  - Entry must be above midpoint for shorts, below midpoint for longs
  - Cannot enter if price is already past the midpoint on the wrong side
  - SL = most recent swing high (short) / swing low (long) at entry time
  - TP1 = midpoint (PSL + range/2); skip if distance to midpoint < MIN_TP1_DISTANCE
  - TP2 = PSL (sweep of highs / short) or PSH (sweep of lows / long)
  - After TP1 hit: SL moves to breakeven for remaining half
  - If BOS flips direction after confirmation: reset to Phase 2 (keep sweep)
  - Minimum FVG size enforced
"""

import pandas as pd

# --- Constants ---
SESSION_START_HOUR  = 3    # 3:00 AM ET  — start building PSH/PSL
SESSION_END_HOUR    = 8    # 8:00 AM ET  — PSH/PSL locked in
NY_START_HOUR       = 8    # 8:00 AM ET  — sweep + entry window opens
NO_TRADE_HOUR       = 13   # 1:00 PM ET  — no new entries at or after this hour

MIN_FVG_SIZE_EURUSD = 0.0003   # 3 pips minimum FVG gap
MIN_FVG_SIZE_USDJPY = 0.03     # 3 pips equivalent for JPY pairs
MIN_TP1_DISTANCE    = 0.0005   # if entry→midpoint < this, skip TP1


def _min_fvg(pair: str) -> float:
    return MIN_FVG_SIZE_USDJPY if "JPY" in pair.upper() else MIN_FVG_SIZE_EURUSD


# ---------------------------------------------------------------------------
# FVG helper
# ---------------------------------------------------------------------------

def _detect_fvg(c1_high, c1_low, c3_high, c3_low, min_size: float):
    """
    Given candle 1 and candle 3 (candle 2 is between them), return FVG info
    or None.

    Bullish FVG: gap between c1 top wick and c3 bottom wick → c3_low > c1_high
    Bearish FVG: gap between c1 bottom wick and c3 top wick → c3_high < c1_low

    Returns dict {direction, fvg_high, fvg_low, size} or None.
    """
    # Bullish FVG
    if c3_low > c1_high:
        size = c3_low - c1_high
        if size >= min_size:
            return {"direction": "bullish", "fvg_high": c3_low, "fvg_low": c1_high, "size": size}

    # Bearish FVG
    if c3_high < c1_low:
        size = c1_low - c3_high
        if size >= min_size:
            return {"direction": "bearish", "fvg_high": c1_low, "fvg_low": c3_high, "size": size}

    return None


# ---------------------------------------------------------------------------
# Main per-day function
# ---------------------------------------------------------------------------

def run_day(day_candles: pd.DataFrame, pair: str) -> dict | None:
    """
    Run the full strategy state machine for a single trading day.

    Args:
        day_candles: 5-min OHLC DataFrame for this day, datetime-indexed,
                     timezone-aware in America/New_York, sorted ascending.
        pair:        "EURUSD" or "USDJPY"

    Returns:
        Trade result dict or None if no valid trade was found.
    """
    candles = day_candles.reset_index(drop=True)
    n = len(candles)
    min_fvg = _min_fvg(pair)

    # ------------------------------------------------------------------
    # Phase 0 — Build PSH / PSL from 3:00–8:00 AM candles
    # ------------------------------------------------------------------
    session_mask = (
        (candles["datetime"].dt.hour >= SESSION_START_HOUR) &
        (candles["datetime"].dt.hour <  SESSION_END_HOUR)
    )
    session_candles = candles[session_mask]

    if session_candles.empty:
        return None

    PSH = session_candles["high"].max()
    PSL = session_candles["low"].min()
    rng = PSH - PSL
    if rng <= 0:
        return None

    midpoint = PSL + rng / 2.0

    # ------------------------------------------------------------------
    # State machine variables
    # ------------------------------------------------------------------
    sweep_detected   = False
    sweep_direction  = None   # "high" or "low"

    # After sweep: track most-recent swing high/low for BOS
    # We update these as new candles form post-sweep
    recent_high = None
    recent_low  = None

    bos_detected     = False
    trade_direction  = None   # "short" or "long"

    # Active FVGs: list of dicts {direction, fvg_high, fvg_low, size, confirmed, invalidated}
    active_fvgs: list[dict] = []

    confirmation_done   = False
    confirmation_fvg    = None   # the FVG/IFVG that confirmed the setup

    continuation_done   = False

    # ------------------------------------------------------------------
    # Phase 1–5: Scan NY session candles
    # ------------------------------------------------------------------
    ny_start_idx = None
    for i in range(n):
        if candles["datetime"][i].hour >= NY_START_HOUR:
            ny_start_idx = i
            break

    if ny_start_idx is None:
        return None

    # We need at least 3 candles for FVG detection, start from index 2 of NY
    for i in range(ny_start_idx, n):
        c = candles.iloc[i]
        dt = c["datetime"]

        # Hard stop: no new entries at or after 1 PM
        if dt.hour >= NO_TRADE_HOUR:
            break

        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]

        # ----------------------------------------------------------------
        # Phase 1 — Sweep detection
        # ----------------------------------------------------------------
        if not sweep_detected:
            swept_high = h > PSH or cl > PSH   # wick or close above PSH
            swept_low  = l < PSL or cl < PSL   # wick or close below PSL

            if swept_high and swept_low:
                # Both swept in same candle — take the one with larger extension
                if (h - PSH) >= (PSL - l):
                    sweep_detected  = True
                    sweep_direction = "high"
                else:
                    sweep_detected  = True
                    sweep_direction = "low"
            elif swept_high:
                sweep_detected  = True
                sweep_direction = "high"
            elif swept_low:
                sweep_detected  = True
                sweep_direction = "low"

            if sweep_detected:
                # Seed most-recent high/low from the sweep candle itself
                recent_high = h
                recent_low  = l
                trade_direction = "short" if sweep_direction == "high" else "long"
            continue   # Move to next candle after marking sweep

        # ----------------------------------------------------------------
        # Phase 2 — BOS detection
        # (runs concurrently with FVG tracking once sweep is detected)
        # ----------------------------------------------------------------

        # Save the high/low from BEFORE this candle for BOS comparison,
        # then update the running trackers with the current candle.
        prev_recent_high = recent_high
        prev_recent_low  = recent_low
        recent_high = max(recent_high, h)
        recent_low  = min(recent_low,  l)

        # Track FVGs — only when all three candles (i-2, i-1, i) are in the NY session
        if i - 2 >= ny_start_idx:
            c1 = candles.iloc[i - 2]
            c3 = c
            fvg = _detect_fvg(c1["high"], c1["low"], c3["high"], c3["low"], min_fvg)
            if fvg is not None:
                active_fvgs.append({**fvg, "formed_at": i, "invalidated": False})

        # Check for IFVG: a candle body closes fully THROUGH an existing FVG.
        # Bullish FVG closed through downward → bearish IFVG (bearish signal)
        # Bearish FVG closed through upward   → bullish IFVG (bullish signal)
        for fvg in active_fvgs:
            if fvg["invalidated"]:
                continue
            if fvg["direction"] == "bullish" and cl < fvg["fvg_low"]:
                fvg["direction"] = "bearish_ifvg"
            elif fvg["direction"] == "bearish" and cl > fvg["fvg_high"]:
                fvg["direction"] = "bullish_ifvg"

        # Invalidate opposite-direction FVGs that have been closed through
        # For shorts: bullish signals (bullish FVG or bullish IFVG) are blockers
        # For longs:  bearish signals (bearish FVG or bearish IFVG) are blockers
        if trade_direction == "short":
            for fvg in active_fvgs:
                if fvg["invalidated"]:
                    continue
                if fvg["direction"] in ("bullish", "bullish_ifvg"):
                    if cl < fvg["fvg_low"]:
                        fvg["invalidated"] = True
        else:  # long
            for fvg in active_fvgs:
                if fvg["invalidated"]:
                    continue
                if fvg["direction"] in ("bearish", "bearish_ifvg"):
                    if cl > fvg["fvg_high"]:
                        fvg["invalidated"] = True

        if not bos_detected:
            # BOS: close through the most recent high/low established since the sweep
            # Uses prev_recent_high/low (values before this candle) so the BOS
            # candle itself can be the one that breaks the level.
            bos_bear = (trade_direction == "short") and (cl < prev_recent_low)
            bos_bull = (trade_direction == "long")  and (cl > prev_recent_high)

            if bos_bear or bos_bull:
                bos_detected = True
                recent_high  = h
                recent_low   = l
            continue

        # ----------------------------------------------------------------
        # Phase 3 — Confirmation FVG / IFVG
        # ----------------------------------------------------------------
        if not confirmation_done:
            # Check if an opposite-direction FVG blocks the trade
            has_blocking_fvg = False
            if trade_direction == "short":
                for fvg in active_fvgs:
                    if not fvg["invalidated"] and fvg["direction"] in ("bullish", "bullish_ifvg"):
                        has_blocking_fvg = True
                        break
            else:
                for fvg in active_fvgs:
                    if not fvg["invalidated"] and fvg["direction"] in ("bearish", "bearish_ifvg"):
                        has_blocking_fvg = True
                        break

            if has_blocking_fvg:
                continue   # Wait for blocking FVG to be invalidated

            # Look for a confirming FVG/IFVG in the trade direction
            for fvg in reversed(active_fvgs):  # most recent first
                if fvg["invalidated"]:
                    continue
                if trade_direction == "short" and fvg["direction"] in ("bearish", "bearish_ifvg"):
                    confirmation_done = True
                    confirmation_fvg  = fvg
                    break
                if trade_direction == "long" and fvg["direction"] in ("bullish", "bullish_ifvg"):
                    confirmation_done = True
                    confirmation_fvg  = fvg
                    break

            if not confirmation_done:
                continue

        # ----------------------------------------------------------------
        # Phase 4 — Continuation confluence (second FVG or EQ)
        # ----------------------------------------------------------------
        if not continuation_done:
            # Option A: Another FVG in trade direction (different from confirmation)
            for fvg in reversed(active_fvgs):
                if fvg["invalidated"]:
                    continue
                if fvg is confirmation_fvg:
                    continue  # must be a different FVG
                if trade_direction == "short" and fvg["direction"] in ("bearish", "bearish_ifvg"):
                    # Bearish candle close confirms continuation
                    if cl < o:
                        continuation_done = True
                        break
                if trade_direction == "long" and fvg["direction"] in ("bullish", "bullish_ifvg"):
                    # Bullish candle close confirms continuation
                    if cl > o:
                        continuation_done = True
                        break

            # Option B: EQ reached with a confirming close
            if not continuation_done:
                # EQ of the most recent impulse move
                eq_level = (recent_high + recent_low) / 2.0

                if trade_direction == "short":
                    # Price reaches EQ from above, candle closes bearish at/near EQ
                    if l <= eq_level and cl < o:
                        continuation_done = True

                elif trade_direction == "long":
                    # Price reaches EQ from below, candle closes bullish at/near EQ
                    if h >= eq_level and cl > o:
                        continuation_done = True

            if not continuation_done:
                # Check: did structure flip? (BOS in wrong direction after confirmation)
                structure_flip_bear = (trade_direction == "long")  and (cl < recent_low)
                structure_flip_bull = (trade_direction == "short") and (cl > recent_high)

                if structure_flip_bear or structure_flip_bull:
                    # Reset to post-sweep state; swap direction
                    trade_direction     = "short" if structure_flip_bear else "long"
                    bos_detected        = False
                    confirmation_done   = False
                    confirmation_fvg    = None
                    continuation_done   = False
                    recent_high         = h
                    recent_low          = l
                continue

        # ----------------------------------------------------------------
        # Phase 5 — Entry validation
        # ----------------------------------------------------------------
        entry = cl

        # Entry constraints
        if trade_direction == "short":
            if entry <= midpoint:
                continue   # must be above midpoint for shorts
        else:
            if entry >= midpoint:
                continue   # must be below midpoint for longs

        # SL: PSH for shorts, PSL for longs
        sl_level = PSH if trade_direction == "short" else PSL

        if trade_direction == "short" and sl_level <= entry:
            continue
        if trade_direction == "long"  and sl_level >= entry:
            continue

        # TP targets
        if trade_direction == "short":
            full_tp = PSL   # sweep of highs → TP at PSL
        else:
            full_tp = PSH   # sweep of lows → TP at PSH

        # Sanity: full TP must be beyond entry
        if trade_direction == "short" and full_tp >= entry:
            continue
        if trade_direction == "long"  and full_tp <= entry:
            continue

        # Minimum R:R filter — skip if reward is less than 1.4× the risk
        risk_dist_check = abs(entry - sl_level)
        if risk_dist_check == 0:
            continue
        if abs(full_tp - entry) / risk_dist_check < 1.4:
            continue

        # TP1 = midpoint of PSH–PSL range
        tp1 = midpoint

        # If entry→TP1 distance too small, skip TP1
        skip_tp1 = abs(entry - tp1) < MIN_TP1_DISTANCE

        # TP1 must also be in the right direction of the trade
        if not skip_tp1:
            if trade_direction == "short" and tp1 >= entry:
                skip_tp1 = True
            if trade_direction == "long"  and tp1 <= entry:
                skip_tp1 = True

        # ------------------------------------------------------------------
        # Phase 6 — Simulate trade forward
        # ------------------------------------------------------------------
        result = _simulate_trade(
            candles       = candles,
            entry_idx     = i,
            entry         = entry,
            stop          = sl_level,
            tp1           = None if skip_tp1 else tp1,
            full_tp       = full_tp,
            is_long       = (trade_direction == "long"),
        )

        risk_dist = abs(entry - sl_level)

        return {
            "direction":        trade_direction.upper(),
            "entry":            round(entry, 5),
            "stop":             round(sl_level, 5),
            "tp1":              None if skip_tp1 else round(tp1, 5),
            "full_tp":          round(full_tp, 5),
            "psh":              round(PSH, 5),
            "psl":              round(PSL, 5),
            "midpoint":         round(midpoint, 5),
            "risk_dist":        round(risk_dist, 5),
            "sweep_direction":  sweep_direction,
            "entry_time":       str(dt),
            "outcome":          result,
        }

    return None   # No valid trade found today


# ---------------------------------------------------------------------------
# Trade simulator
# ---------------------------------------------------------------------------

def _simulate_trade(
    candles,
    entry_idx: int,
    entry: float,
    stop: float,
    tp1: float | None,
    full_tp: float,
    is_long: bool,
) -> dict:
    """
    Simulate a split-lot trade candle-by-candle after the entry candle.
    Runs until SL or TP is hit — no time-based exit.

    Returns:
        {
            profit_raw:  combined price-distance profit (caller converts to $)
            label:       'W'  both halves hit TP
                         'PW' TP1 hit, half 2 stopped at breakeven
                         'L'  stopped out before TP1
        }
    """
    tp1_hit      = False
    tp1_profit   = 0.0
    active_stop  = stop

    for j in range(entry_idx + 1, len(candles)):
        c  = candles.iloc[j]

        lo = c["low"]
        hi = c["high"]

        if is_long:
            # Stop hit
            if lo <= active_stop:
                half2_profit = active_stop - entry   # 0 at BE, negative at original stop
                return {
                    "profit_raw": tp1_profit + half2_profit,
                    "label": "PW" if tp1_hit else "L",
                }
            # TP1
            if tp1 is not None and not tp1_hit and hi >= tp1:
                tp1_profit  = tp1 - entry
                tp1_hit     = True
                active_stop = entry   # move SL to breakeven
            # Full TP
            if tp1_hit and hi >= full_tp:
                half2_profit = full_tp - entry
                return {"profit_raw": tp1_profit + half2_profit, "label": "W"}
            # If TP1 was skipped, look straight for full TP
            if tp1 is None and hi >= full_tp:
                return {"profit_raw": full_tp - entry, "label": "W"}

        else:  # SHORT
            # Stop hit
            if hi >= active_stop:
                half2_profit = entry - active_stop
                return {
                    "profit_raw": tp1_profit + half2_profit,
                    "label": "PW" if tp1_hit else "L",
                }
            # TP1
            if tp1 is not None and not tp1_hit and lo <= tp1:
                tp1_profit  = entry - tp1
                tp1_hit     = True
                active_stop = entry
            # Full TP
            if tp1_hit and lo <= full_tp:
                half2_profit = entry - full_tp
                return {"profit_raw": tp1_profit + half2_profit, "label": "W"}
            # If TP1 was skipped, look straight for full TP
            if tp1 is None and lo <= full_tp:
                return {"profit_raw": entry - full_tp, "label": "W"}

    # Ran out of candle data — if TP1 was hit, half 2 is still at breakeven
    if tp1_hit:
        return {"profit_raw": tp1_profit, "label": "PW"}

    return {"profit_raw": 0.0, "label": "L"}
