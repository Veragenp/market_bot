from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from trading_bot.config.settings import DEFAULT_SOURCE_BINANCE, LEVEL_EVENTS_LOOKBACK_HOURS, LEVEL_EVENTS_WINDOW_HOURS
from trading_bot.data.db import get_connection
from trading_bot.data.repositories import get_ohlcv
from trading_bot.data.schema import init_db, run_migrations
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
        SELECT symbol, MAX(created_at) AS mx
        FROM price_levels
        WHERE is_active = 1 AND level_type = 'volume_profile_peaks'
        GROUP BY symbol
        """
    ).fetchall():
        symbol = str(sym_row["symbol"])
        created_at = int(sym_row["mx"])
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
                   price, volume_peak, duration_hours, t_end_unix
            FROM price_levels
            WHERE symbol = ? AND is_active = 1 AND level_type = 'volume_profile_peaks' AND created_at = ?
            """,
            (symbol, created_at),
        ).fetchall():
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

    # Earliest touch across all symbols/levels => global t0
    touches: List[tuple[int, LevelRow, int, float]] = []
    for lv in all_levels:
        candles = get_ohlcv(lv.symbol, "1m", start=lookback_start, end=now, source=DEFAULT_SOURCE_BINANCE)
        if len(candles) < 3:
            continue
        first_close = float(candles[0]["close"])
        dist_start = abs(first_close - lv.price) / lv.atr_daily if lv.atr_daily > 0 else 0.0
        if dist_start < 0.3:
            continue
        prev = float(candles[0]["close"])
        for idx in range(1, len(candles)):
            cur = float(candles[idx]["close"])
            touched = (prev <= lv.price <= cur) or (prev >= lv.price >= cur) or abs(cur - lv.price) <= 0.05 * lv.atr_daily
            if touched:
                ts = int(candles[idx]["timestamp"])
                touches.append((ts, lv, idx, dist_start))
                break
            prev = cur
    if not touches:
        return []

    t0 = min(t[0] for t in touches)
    window_end = t0 + int(LEVEL_EVENTS_WINDOW_HOURS * 3600)
    events: List[Dict[str, object]] = []

    for lv in all_levels:
        candles = get_ohlcv(lv.symbol, "1m", start=t0 - 60, end=window_end, source=DEFAULT_SOURCE_BINANCE)
        if len(candles) < 3 or lv.atr_daily <= 0:
            continue
        price_at_t0 = float(candles[1]["close"])
        dist_start = abs(price_at_t0 - lv.price) / lv.atr_daily
        if dist_start < 0.3:
            continue
        eps = 0.01 * lv.atr_daily
        prev_close = float(candles[0]["close"])
        event_counter = 0
        i = 1
        while i < len(candles):
            cur_close = float(candles[i]["close"])
            crossed = (prev_close <= lv.price <= cur_close) or (prev_close >= lv.price >= cur_close)
            if not crossed:
                prev_close = cur_close
                i += 1
                continue
            touch_ts = int(candles[i]["timestamp"])
            pre_side = 1 if prev_close > lv.price else -1
            extreme_behind = cur_close
            rebound_pure = 0.0
            return_ts = None
            rebound_after = None
            j = i
            while j < len(candles):
                p = float(candles[j]["close"])
                if pre_side > 0:
                    if p < lv.price:
                        extreme_behind = min(extreme_behind, p)
                    if p > lv.price:
                        rebound_pure = max(rebound_pure, (p - lv.price) / lv.atr_daily)
                    if p >= lv.price + eps:
                        return_ts = int(candles[j]["timestamp"])
                        break
                else:
                    if p > lv.price:
                        extreme_behind = max(extreme_behind, p)
                    if p < lv.price:
                        rebound_pure = max(rebound_pure, (lv.price - p) / lv.atr_daily)
                    if p <= lv.price - eps:
                        return_ts = int(candles[j]["timestamp"])
                        break
                j += 1
            if pre_side > 0:
                penetration = max(0.0, (lv.price - extreme_behind) / lv.atr_daily)
            else:
                penetration = max(0.0, (extreme_behind - lv.price) / lv.atr_daily)

            if return_ts is not None:
                after_vals = [float(c["close"]) for c in candles[j:min(j + 240, len(candles))]]
                if after_vals:
                    if pre_side > 0:
                        rebound_after = max(0.0, (max(after_vals) - lv.price) / lv.atr_daily)
                    else:
                        rebound_after = max(0.0, (lv.price - min(after_vals)) / lv.atr_daily)

            sid = stable_level_id(
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
                    "volume_peak": lv.volume_peak,
                    "duration_hours": lv.duration_hours,
                    "atr_daily": lv.atr_daily,
                    "dist_start_atr": dist_start,
                    "touch_time": touch_ts,
                    "return_time": return_ts,
                    "penetration_atr": penetration,
                    "rebound_pure_atr": rebound_pure,
                    "rebound_after_return_atr": rebound_after,
                    "window_start": t0,
                    "window_end": window_end,
                }
            )
            prev_close = cur_close
            i = j + 1 if j > i else i + 1

    # cluster size in 4h from touch_time (same symbol-agnostic pool)
    touch_times = sorted(int(e["touch_time"]) for e in events)
    for e in events:
        t = int(e["touch_time"])
        e["cluster_size"] = sum(1 for x in touch_times if t <= x <= t + int(LEVEL_EVENTS_WINDOW_HOURS * 3600))
    return events

