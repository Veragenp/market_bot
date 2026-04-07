from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from trading_bot.config.settings import (
    DEFAULT_SOURCE_BINANCE,
    LEVEL_EVENTS_CONFIRM_ATR_PCT,
    LEVEL_EVENTS_LOOKBACK_HOURS,
    LEVEL_EVENTS_MIN_PENETRATION_ATR,
    LEVEL_EVENTS_REBOUND_HORIZON_BARS,
    LEVEL_EVENTS_RETURN_EPS_ATR,
    LEVEL_EVENTS_STALE_OPEN_MINUTES,
    LEVEL_EVENTS_WINDOW_HOURS,
)
from trading_bot.data.db import get_connection
from trading_bot.data.repositories import get_ohlcv
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.volume_profile_peaks_db import LEVEL_TYPE_VP_LOCAL
from trading_bot.analytics.level_identity import stable_level_id


@dataclass
class LevelRow:
    symbol: str
    level_type: str
    layer: str
    tier: str
    price: float
    volume_peak: Optional[float]
    duration_hours: Optional[float]
    t_end_unix: Optional[int]
    tick_size: float
    atr_daily: float
    stable_level_id_db: Optional[str] = None


def _month_label(ts: Optional[int]) -> str:
    if ts is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return f"{dt.year}-{dt.month:02d}"


def load_active_levels_with_metrics() -> List[LevelRow]:
    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    rows: List[LevelRow] = []
    for sym_row in cur.execute(
        """
        SELECT DISTINCT symbol
        FROM price_levels
        WHERE is_active = 1 AND status = 'active' AND level_type = ?
        """,
        (LEVEL_TYPE_VP_LOCAL,),
    ).fetchall():
        symbol = str(sym_row["symbol"])
        inst = cur.execute(
            "SELECT tick_size, atr FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
            (symbol.replace("/", ""),),
        ).fetchone()
        if not inst or inst["atr"] is None:
            continue
        tick_size = float(inst["tick_size"] or 1e-8)
        atr_daily = float(inst["atr"])
        for r in cur.execute(
            """
            SELECT symbol, level_type, ifnull(layer,'') AS layer, ifnull(tier,'') AS tier,
                   price, volume_peak, duration_hours, t_end_unix, stable_level_id
            FROM price_levels
            WHERE symbol = ? AND is_active = 1 AND status = 'active' AND level_type = ?
            """,
            (symbol, LEVEL_TYPE_VP_LOCAL),
        ).fetchall():
            sid = r["stable_level_id"]
            rows.append(
                LevelRow(
                    symbol=str(r["symbol"]),
                    level_type=str(r["level_type"]),
                    layer=str(r["layer"]),
                    tier=str(r["tier"]),
                    price=float(r["price"]),
                    volume_peak=float(r["volume_peak"]) if r["volume_peak"] is not None else None,
                    duration_hours=float(r["duration_hours"]) if r["duration_hours"] is not None else None,
                    t_end_unix=int(r["t_end_unix"]) if r["t_end_unix"] is not None else None,
                    tick_size=tick_size,
                    atr_daily=atr_daily,
                    stable_level_id_db=str(sid) if sid else None,
                )
            )
    conn.close()
    return rows


def build_level_events(now_ts: Optional[int] = None) -> List[Dict[str, object]]:
    now = int(now_ts or time.time())
    lookback_start = now - int(LEVEL_EVENTS_LOOKBACK_HOURS * 3600)
    all_levels = load_active_levels_with_metrics()
    if not all_levels:
        return []

    symbol_candles: Dict[str, List[Dict[str, object]]] = {}
    for lv in all_levels:
        if lv.symbol not in symbol_candles:
            symbol_candles[lv.symbol] = get_ohlcv(
                lv.symbol,
                "1m",
                start=lookback_start,
                end=now,
                source=DEFAULT_SOURCE_BINANCE,
            )

    events: List[Dict[str, object]] = []
    min_pen_atr = float(LEVEL_EVENTS_MIN_PENETRATION_ATR)
    eps_mult = float(LEVEL_EVENTS_RETURN_EPS_ATR)
    rebound_horizon = int(LEVEL_EVENTS_REBOUND_HORIZON_BARS)

    for lv in all_levels:
        candles = symbol_candles.get(lv.symbol) or []
        if len(candles) < 3 or lv.atr_daily <= 0:
            continue

        price_at_start = float(candles[0]["close"])
        dist_start = abs(price_at_start - lv.price) / lv.atr_daily
        eps = eps_mult * lv.atr_daily
        confirm_delta = float(LEVEL_EVENTS_CONFIRM_ATR_PCT) * lv.atr_daily
        event_counter = 0
        i = 1
        while i < len(candles):
            low_i = float(candles[i]["low"])
            high_i = float(candles[i]["high"])
            prev_close = float(candles[i - 1]["close"])

            is_touch = (
                low_i <= lv.price <= high_i
                or abs(lv.price - low_i) <= 0.01 * lv.atr_daily
                or abs(lv.price - high_i) <= 0.01 * lv.atr_daily
            )
            if not is_touch:
                i += 1
                continue

            touch_ts = int(candles[i]["timestamp"])
            pre_side = 1 if prev_close > lv.price else -1
            pre_side_label = "from_above" if pre_side > 0 else "from_below"
            extreme_behind = lv.price
            rebound_pure = 0.0
            return_ts = None
            rebound_after = None
            event_status = "open"
            confirm_time = None
            touch_count_before_confirm = 1

            # Do not allow immediate return on the same candle.
            j = i + 1
            while j < len(candles):
                touch_hit = (
                    float(candles[j]["low"]) <= lv.price <= float(candles[j]["high"])
                )
                if touch_hit:
                    touch_count_before_confirm += 1
                if pre_side > 0:
                    low_j = float(candles[j]["low"])
                    high_j = float(candles[j]["high"])
                    extreme_behind = min(extreme_behind, low_j)
                    rebound_pure = max(rebound_pure, (high_j - lv.price) / lv.atr_daily)
                    if low_j <= lv.price - confirm_delta:
                        event_status = "confirmed_breakout_down"
                        confirm_time = int(candles[j]["timestamp"])
                        break
                    if high_j >= lv.price + confirm_delta:
                        event_status = "confirmed_rebound_up"
                        confirm_time = int(candles[j]["timestamp"])
                        break
                    if high_j >= lv.price + eps:
                        return_ts = int(candles[j]["timestamp"])
                else:
                    low_j = float(candles[j]["low"])
                    high_j = float(candles[j]["high"])
                    extreme_behind = max(extreme_behind, high_j)
                    rebound_pure = max(rebound_pure, (lv.price - low_j) / lv.atr_daily)
                    if high_j >= lv.price + confirm_delta:
                        event_status = "confirmed_breakout_up"
                        confirm_time = int(candles[j]["timestamp"])
                        break
                    if low_j <= lv.price - confirm_delta:
                        event_status = "confirmed_rebound_down"
                        confirm_time = int(candles[j]["timestamp"])
                        break
                    if low_j <= lv.price - eps:
                        return_ts = int(candles[j]["timestamp"])
                j += 1

            # If no return found, keep tracking penetration till the end.
            if confirm_time is None:
                k = max(i + 1, j)
                while k < len(candles):
                    if pre_side > 0:
                        extreme_behind = min(extreme_behind, float(candles[k]["low"]))
                    else:
                        extreme_behind = max(extreme_behind, float(candles[k]["high"]))
                    k += 1
                last_ts = int(candles[-1]["timestamp"])
                if (last_ts - touch_ts) >= int(LEVEL_EVENTS_STALE_OPEN_MINUTES) * 60:
                    event_status = "stale_open"

            if confirm_time is None and return_ts is not None:
                event_status = "false_break"

            if pre_side > 0:
                penetration = max(0.0, (lv.price - extreme_behind) / lv.atr_daily)
            else:
                penetration = max(0.0, (extreme_behind - lv.price) / lv.atr_daily)

            if penetration < min_pen_atr:
                i = j + 1 if j > i else i + 1
                continue

            if confirm_time is not None:
                return_ts = int(confirm_time)
            if return_ts is not None:
                after_vals = candles[j : min(j + rebound_horizon, len(candles))]
                if after_vals:
                    if pre_side > 0:
                        max_after = max(float(c["high"]) for c in after_vals)
                        rebound_after = max(0.0, (max_after - lv.price) / lv.atr_daily)
                    else:
                        min_after = min(float(c["low"]) for c in after_vals)
                        rebound_after = max(0.0, (lv.price - min_after) / lv.atr_daily)

            sid = lv.stable_level_id_db or stable_level_id(
                symbol=lv.symbol,
                level_type=lv.level_type,
                layer=lv.layer,
                tier=lv.tier,
                price=lv.price,
                tick_size=lv.tick_size,
            )
            event_counter += 1
            eid = f"{sid}_{touch_ts}_{event_counter}"
            events.append(
                {
                    "event_id": eid,
                    "stable_level_id": sid,
                    "symbol": lv.symbol,
                    "month_utc": _month_label(lv.t_end_unix),
                    "level_type": lv.level_type,
                    "layer": lv.layer,
                    "tier": lv.tier,
                    "level_price": lv.price,
                    "event_status": event_status,
                    "pre_side": pre_side_label,
                    "volume_peak": lv.volume_peak,
                    "duration_hours": lv.duration_hours,
                    "atr_daily": lv.atr_daily,
                    "atr_pct": (lv.atr_daily / lv.price * 100.0) if lv.price else None,
                    "dist_start_atr": dist_start,
                    "touch_time": touch_ts,
                    "return_time": return_ts,
                    "penetration_atr": penetration,
                    "penetration_pct": (penetration * lv.atr_daily / lv.price * 100.0) if lv.price else None,
                    "rebound_pure_atr": rebound_pure,
                    "rebound_pure_pct": (rebound_pure * lv.atr_daily / lv.price * 100.0) if lv.price else None,
                    "rebound_after_return_atr": rebound_after,
                    "rebound_after_return_pct": (
                        (float(rebound_after) * lv.atr_daily / lv.price * 100.0)
                        if (rebound_after is not None and lv.price)
                        else None
                    ),
                    "confirm_time": confirm_time,
                    "confirm_time_sec": (int(confirm_time) - touch_ts) if confirm_time is not None else None,
                    "touch_count_before_confirm": int(touch_count_before_confirm),
                    "window_start": lookback_start,
                    "window_end": now,
                }
            )
            i = j + 1 if j > i else i + 1

    # cluster size in LEVEL_EVENTS_WINDOW_HOURS from touch_time (sliding window).
    if events:
        window_sec = int(LEVEL_EVENTS_WINDOW_HOURS * 3600)
        events_sorted = sorted(events, key=lambda x: int(x["touch_time"]))
        times = [int(e["touch_time"]) for e in events_sorted]
        left = 0
        for right, t in enumerate(times):
            while times[left] < t - window_sec:
                left += 1
            events_sorted[right]["cluster_size"] = right - left + 1
    return events

