from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from trading_bot.config.settings import (
    CYCLE_LEVELS_ALLOWED_LEVEL_TYPES,
    CYCLE_LEVELS_COOLDOWN_HOURS,
    CYCLE_LEVELS_CANDIDATES_WORKSHEET,
    CYCLE_LEVELS_DIAG_WORKSHEET,
    CYCLE_LEVELS_FALLBACK_MAX_ATR,
    CYCLE_LEVELS_MIN_DIST_ATR,
    CYCLE_LEVELS_REBUILD_ENABLED,
    CYCLE_LEVELS_WORKSHEET,
    CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS,
    CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR,
    CYCLE_LEVELS_ZONE_HALF_WIDTH_ATR,
    STRUCTURAL_MIN_POOL_SYMBOLS,
    STRUCTURAL_N_ETALON,
    STRUCTURAL_TOP_K,
    STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT,
    STRUCTURAL_W_MAX,
    STRUCTURAL_W_MIN,
    ALL_ACTIVE_LEVELS_WORKSHEET 
)
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.tools.price_feed import PricePoint, get_price_feed
from trading_bot.tools.sheets_exporter import SheetsExporter
# Определяем имя листа для всех активных уровней (если не задано в .env)
ALL_ACTIVE_LEVELS_WORKSHEET = os.getenv("ALL_ACTIVE_LEVELS_WORKSHEET", "all_active_levels")

@dataclass
class Candidate:
    id: int
    symbol: str
    direction: str
    price: float
    volume_peak: float
    strength: float
    level_type: str
    source_is_manual_global: int
    tier: str
    updated_at: int
    distance_atr: float


def _now_ts() -> int:
    return int(time.time())


def _latest_close_1m(symbol: str, cur) -> Optional[float]:
    row = cur.execute(
        """
        SELECT close
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if row is None or row["close"] is None:
        return None
    return float(row["close"])


def _atr_daily(symbol: str, cur) -> Optional[float]:
    return get_instruments_atr_bybit_futures_cur(cur, symbol)


def _load_candidates(symbol: str, direction: str, cur) -> List[Candidate]:
    types = list(CYCLE_LEVELS_ALLOWED_LEVEL_TYPES)
    if not types:
        return []
    px = _latest_close_1m(symbol, cur)
    atr = _atr_daily(symbol, cur)
    if px is None or atr is None:
        return []
    op = "<" if direction == "long" else ">"
    ph = ",".join("?" * len(types))
    rows = cur.execute(
        f"""
        SELECT id, symbol, price, volume_peak, strength, level_type, tier,
               COALESCE(updated_at, created_at) AS upd
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price {op} ?
        ORDER BY COALESCE(volume_peak, 0) DESC, COALESCE(strength, 0) DESC,
                 COALESCE(updated_at, created_at) DESC
        """,
        (symbol, *types, px),
    ).fetchall()
    out: List[Candidate] = []
    for r in rows:
        price = float(r["price"])
        dist = abs(price - px) / atr
        out.append(
            Candidate(
                id=int(r["id"]),
                symbol=str(r["symbol"]),
                direction=direction,
                price=price,
                volume_peak=float(r["volume_peak"] or 0.0),
                strength=float(r["strength"] or 0.0),
                level_type=str(r["level_type"] or ""),
                source_is_manual_global=1 if str(r["level_type"] or "") == "manual_global_hvn" else 0,
                tier=str(r["tier"] or ""),
                updated_at=int(r["upd"] or 0),
                distance_atr=float(dist),
            )
        )
    return out


def _load_candidates_with_ref_price(
    symbol: str,
    direction: str,
    ref_price: float,
    atr: float,
    cur,
) -> List[Candidate]:
    types = list(CYCLE_LEVELS_ALLOWED_LEVEL_TYPES)
    if not types:
        return []
    op = "<" if direction == "long" else ">"
    ph = ",".join("?" * len(types))
    rows = cur.execute(
        f"""
        SELECT id, symbol, price, volume_peak, strength, level_type, tier,
               COALESCE(updated_at, created_at) AS upd
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price {op} ?
        ORDER BY COALESCE(volume_peak, 0) DESC, COALESCE(strength, 0) DESC,
                 COALESCE(updated_at, created_at) DESC
        """,
        (symbol, *types, ref_price),
    ).fetchall()
    out: List[Candidate] = []
    for r in rows:
        price = float(r["price"])
        dist = abs(price - ref_price) / atr
        out.append(
            Candidate(
                id=int(r["id"]),
                symbol=str(r["symbol"]),
                direction=direction,
                price=price,
                volume_peak=float(r["volume_peak"] or 0.0),
                strength=float(r["strength"] or 0.0),
                level_type=str(r["level_type"] or ""),
                source_is_manual_global=1 if str(r["level_type"] or "") == "manual_global_hvn" else 0,
                tier=str(r["tier"] or ""),
                updated_at=int(r["upd"] or 0),
                distance_atr=float(dist),
            )
        )
    return out


def _is_on_cooldown(c: Candidate, cur, now_ts: int) -> bool:
    cooldown_sec = int(CYCLE_LEVELS_COOLDOWN_HOURS * 3600)
    row = cur.execute(
        """
        SELECT used_at
        FROM cycle_level_usage
        WHERE symbol = ? AND direction = ? AND source_level_id = ?
        ORDER BY used_at DESC
        LIMIT 1
        """,
        (c.symbol, c.direction, c.id),
    ).fetchone()
    if row is None or row["used_at"] is None:
        return False
    return int(now_ts) - int(row["used_at"]) < cooldown_sec


def _pick_best(cands: List[Candidate], min_dist: float, max_dist: float, cur, now_ts: int) -> Optional[Candidate]:
    filtered = [
        c
        for c in cands
        if c.distance_atr >= float(CYCLE_LEVELS_MIN_DIST_ATR)
        and c.distance_atr >= min_dist
        and c.distance_atr <= max_dist
        and not _is_on_cooldown(c, cur, now_ts)
    ]
    if not filtered:
        return None
    filtered.sort(
        key=lambda x: (-x.volume_peak, -x.strength, x.distance_atr, -x.updated_at)
    )
    return filtered[0]


def _pick_best_with_reason(
    cands: List[Candidate], min_dist: float, max_dist: float, cur, now_ts: int
) -> Tuple[Optional[Candidate], str]:
    if not cands:
        return None, "no_candidates"
    after_min = [c for c in cands if c.distance_atr >= float(CYCLE_LEVELS_MIN_DIST_ATR)]
    if not after_min:
        return None, "below_min_dist_atr"
    after_cd = [c for c in after_min if not _is_on_cooldown(c, cur, now_ts)]
    if not after_cd:
        return None, "cooldown_block"
    in_zone = [c for c in after_cd if c.distance_atr >= min_dist and c.distance_atr <= max_dist]
    if not in_zone:
        return None, "out_of_zone"
    in_zone.sort(key=lambda x: (-x.volume_peak, -x.strength, x.distance_atr, -x.updated_at))
    return in_zone[0], "picked"


def _pick_strongest(cands: List[Candidate], cur, now_ts: int) -> Optional[Candidate]:
    filtered = [
        c
        for c in cands
        if c.distance_atr >= float(CYCLE_LEVELS_MIN_DIST_ATR) and not _is_on_cooldown(c, cur, now_ts)
    ]
    if not filtered:
        return None
    filtered.sort(key=lambda x: (-x.volume_peak, -x.strength, x.distance_atr, -x.updated_at))
    return filtered[0]


def _load_side_level(cur, *, cycle_id: str, symbol: str, direction: str) -> Optional[float]:
    row = cur.execute(
        """
        SELECT level_price
        FROM cycle_levels
        WHERE cycle_id = ? AND symbol = ? AND direction = ? AND level_step = 1 AND is_active = 1
        LIMIT 1
        """,
        (cycle_id, symbol, direction),
    ).fetchone()
    if not row or row["level_price"] is None:
        return None
    return float(row["level_price"])


def _load_anchor_candidates_with_ref(
    cur,
    *,
    symbol: str,
    missing_direction: str,
    anchor_price: float,
    ref_price: float,
    atr: float,
) -> List[Candidate]:
    types = list(CYCLE_LEVELS_ALLOWED_LEVEL_TYPES)
    if not types:
        return []
    if missing_direction == "short":
        op = ">"
    else:
        op = "<"
    ph = ",".join("?" * len(types))
    rows = cur.execute(
        f"""
        SELECT id, symbol, price, volume_peak, strength, level_type, tier,
               COALESCE(updated_at, created_at) AS upd
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price {op} ?
        ORDER BY COALESCE(volume_peak, 0) DESC, COALESCE(strength, 0) DESC,
                 COALESCE(updated_at, created_at) DESC
        """,
        (symbol, *types, anchor_price),
    ).fetchall()
    out: List[Candidate] = []
    now_ts = _now_ts()
    for r in rows:
        price = float(r["price"])
        dist = abs(price - ref_price) / atr
        c = Candidate(
            id=int(r["id"]),
            symbol=str(r["symbol"]),
            direction=missing_direction,
            price=price,
            volume_peak=float(r["volume_peak"] or 0.0),
            strength=float(r["strength"] or 0.0),
            level_type=str(r["level_type"] or ""),
            source_is_manual_global=1 if str(r["level_type"] or "") == "manual_global_hvn" else 0,
            tier=str(r["tier"] or ""),
            updated_at=int(r["upd"] or 0),
            distance_atr=float(dist),
        )
        if c.distance_atr < float(CYCLE_LEVELS_MIN_DIST_ATR):
            continue
        if _is_on_cooldown(c, cur, now_ts):
            continue
        out.append(c)
    return out


def _median(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    mid = len(s) // 2
    if len(s) % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _direction_zone(cands_by_symbol: Dict[str, List[Candidate]]) -> tuple[float, float]:
    strongest_dist: List[float] = []
    for arr in cands_by_symbol.values():
        if not arr:
            continue
        arr_sorted = sorted(
            arr, key=lambda x: (-x.volume_peak, -x.strength, x.distance_atr, -x.updated_at)
        )
        strongest_dist.append(float(arr_sorted[0].distance_atr))
    if len(strongest_dist) < 5:
        return 0.0, float(CYCLE_LEVELS_FALLBACK_MAX_ATR)
    mu = _median(strongest_dist)
    return max(0.0, mu - float(CYCLE_LEVELS_ZONE_HALF_WIDTH_ATR)), mu + float(CYCLE_LEVELS_ZONE_HALF_WIDTH_ATR)


def rebuild_cycle_levels(*, force: bool = False) -> dict:
    if not CYCLE_LEVELS_REBUILD_ENABLED and not force:
        return {"skipped": True, "reason": "cycle_levels_rebuild_disabled"}

    now_ts = _now_ts()
    cycle_id = str(uuid.uuid4())
    conn = get_connection()
    cur = conn.cursor()

    feed = get_price_feed()
    live_prices: Dict[str, PricePoint] = feed.get_prices(TRADING_SYMBOLS)
    ref_meta: Dict[str, PricePoint] = {}
    by_dir: Dict[str, Dict[str, List[Candidate]]] = {"long": {}, "short": {}}
    for symbol in TRADING_SYMBOLS:
        atr = _atr_daily(symbol, cur)
        if atr is None:
            by_dir["long"][symbol] = []
            by_dir["short"][symbol] = []
            continue
        pp = live_prices.get(symbol)
        if pp is None:
            px = _latest_close_1m(symbol, cur)
            if px is None:
                by_dir["long"][symbol] = []
                by_dir["short"][symbol] = []
                continue
            pp = PricePoint(price=float(px), ts=now_ts, source="db_1m_close")
        ref_meta[symbol] = pp
        by_dir["long"][symbol] = _load_candidates_with_ref_price(symbol, "long", pp.price, atr, cur)
        by_dir["short"][symbol] = _load_candidates_with_ref_price(symbol, "short", pp.price, atr, cur)

    picks: List[Candidate] = []
    for direction in ("long", "short"):
        zmin, zmax = _direction_zone(by_dir[direction])
        for symbol in TRADING_SYMBOLS:
            cands = by_dir[direction][symbol]
            local_zmin = zmin
            local_zmax = zmax
            pick = _pick_best(cands, local_zmin, local_zmax, cur, now_ts)
            if pick is None:
                for _ in range(int(CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS)):
                    local_zmin = max(0.0, local_zmin - float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR))
                    local_zmax = local_zmax + float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR)
                    pick = _pick_best(cands, local_zmin, local_zmax, cur, now_ts)
                    if pick is not None:
                        break
            if pick is not None:
                picks.append(pick)

    cur.execute("DELETE FROM cycle_levels")
    for p in picks:
        ref = ref_meta.get(p.symbol)
        cur.execute(
            """
            INSERT INTO cycle_levels (
                cycle_id, symbol, direction, level_step, level_price, source_level_id,
                tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                is_primary, is_active, frozen_at, updated_at
            )
            VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (
                cycle_id,
                p.symbol,
                p.direction,
                p.price,
                p.id,
                p.tier,
                p.volume_peak,
                p.distance_atr,
                (ref.price if ref is not None else None),
                (ref.source if ref is not None else None),
                (ref.ts if ref is not None else None),
                now_ts,
                now_ts,
            ),
        )

    cur.execute(
        """
        UPDATE trading_state
        SET cycle_id = ?, position_state = 'none', cycle_phase = 'arming',
            levels_frozen = 1, cycle_version = cycle_version + 1,
            close_reason = NULL, last_package_exit_reason = NULL,
            last_transition_at = ?, updated_at = ?
        WHERE id = 1
        """,
        (cycle_id, now_ts, now_ts),
    )
    conn.commit()
    conn.close()
    return {"cycle_id": cycle_id, "rows": len(picks)}


def backfill_missing_cycle_side(
    cur,
    *,
    cycle_id: str,
    symbols: List[str],
    missing_direction: str,
    ref_prices: Dict[str, float],
    ref_source: str = "entry_gate_rebuild",
) -> dict:
    if missing_direction not in ("long", "short"):
        return {"inserted": 0, "missing": list(symbols), "error": "invalid_direction"}
    now_ts = _now_ts()
    fixed_direction = "long" if missing_direction == "short" else "short"
    todo: Dict[str, Dict[str, object]] = {}
    for symbol in symbols:
        atr = _atr_daily(symbol, cur)
        if atr is None or atr <= 0:
            continue
        exists = cur.execute(
            """
            SELECT 1
            FROM cycle_levels
            WHERE cycle_id = ? AND symbol = ? AND direction = ? AND level_step = 1 AND is_active = 1
            LIMIT 1
            """,
            (cycle_id, symbol, missing_direction),
        ).fetchone()
        if exists is not None:
            continue

        ref = ref_prices.get(symbol)
        if ref is None or float(ref) <= 0:
            ref = _latest_close_1m(symbol, cur)
        if ref is None or float(ref) <= 0:
            continue

        fixed_price = _load_side_level(cur, cycle_id=cycle_id, symbol=symbol, direction=fixed_direction)
        if fixed_price is not None:
            cands = _load_anchor_candidates_with_ref(
                cur,
                symbol=symbol,
                missing_direction=missing_direction,
                anchor_price=float(fixed_price),
                ref_price=float(ref),
                atr=float(atr),
            )
        else:
            cands = _load_candidates_with_ref_price(
                symbol=symbol,
                direction=missing_direction,
                ref_price=float(ref),
                atr=float(atr),
                cur=cur,
            )
        if not cands:
            continue
        todo[symbol] = {
            "ref": float(ref),
            "cands": cands[: int(STRUCTURAL_TOP_K)],
            "idx": -1,
        }

    if not todo:
        return {"inserted": 0, "missing": list(symbols)}

    band_mult = max(0.1, float(STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT))
    fit_lo = max(float(CYCLE_LEVELS_MIN_DIST_ATR), float(STRUCTURAL_W_MIN) * band_mult)
    fit_hi = max(fit_lo, float(STRUCTURAL_W_MAX) * band_mult)

    for _symbol, item in todo.items():
        cands = item["cands"]  # type: ignore[index]
        idx_found = -1
        for i, cand in enumerate(cands):
            if fit_lo <= float(cand.distance_atr) <= fit_hi:
                idx_found = i
                break
        if idx_found < 0 and cands:
            idx_found = 0
        item["idx"] = idx_found  # type: ignore[index]

    inserted = 0
    missing: List[str] = []
    for symbol in symbols:
        exists = cur.execute(
            """
            SELECT 1
            FROM cycle_levels
            WHERE cycle_id = ? AND symbol = ? AND direction = ? AND level_step = 1 AND is_active = 1
            LIMIT 1
            """,
            (cycle_id, symbol, missing_direction),
        ).fetchone()
        if exists is not None:
            continue
        item = todo.get(symbol)
        if item is None:
            missing.append(symbol)
            continue
        cands = item["cands"]  # type: ignore[index]
        idx = int(item["idx"])  # type: ignore[index]
        if idx < 0 or idx >= len(cands):
            missing.append(symbol)
            continue
        cand = cands[idx]
        ref = float(item["ref"])  # type: ignore[index]
        cur.execute(
            """
            INSERT INTO cycle_levels (
                cycle_id, symbol, direction, level_step, level_price, source_level_id,
                tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                is_primary, is_active, frozen_at, updated_at
            )
            VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (
                cycle_id,
                symbol,
                missing_direction,
                cand.price,
                cand.id,
                cand.tier,
                cand.volume_peak,
                cand.distance_atr,
                ref,
                ref_source,
                now_ts,
                now_ts,
                now_ts,
            ),
        )
        inserted += 1

    for symbol in symbols:
        exists = cur.execute(
            """
            SELECT 1
            FROM cycle_levels
            WHERE cycle_id = ? AND symbol = ? AND direction = ? AND level_step = 1 AND is_active = 1
            LIMIT 1
            """,
            (cycle_id, symbol, missing_direction),
        ).fetchone()
        if exists is None:
            missing.append(symbol)
    return {"inserted": inserted, "missing": sorted(set(missing))}


def build_cycle_levels_diagnostics():
    now_ts = _now_ts()
    conn = get_connection()
    cur = conn.cursor()
    feed = get_price_feed()
    live_prices: Dict[str, PricePoint] = feed.get_prices(TRADING_SYMBOLS)
    rows: List[dict] = []
    for direction in ("long", "short"):
        cands_by_symbol: Dict[str, List[Candidate]] = {}
        for symbol in TRADING_SYMBOLS:
            atr = _atr_daily(symbol, cur)
            if atr is None:
                cands_by_symbol[symbol] = []
                continue
            pp = live_prices.get(symbol)
            if pp is None:
                px = _latest_close_1m(symbol, cur)
                if px is None:
                    cands_by_symbol[symbol] = []
                    continue
                pp = PricePoint(price=float(px), ts=now_ts, source="db_1m_close")
            cands_by_symbol[symbol] = _load_candidates_with_ref_price(
                symbol, direction, pp.price, atr, cur
            )
        zmin, zmax = _direction_zone(cands_by_symbol)
        for symbol in TRADING_SYMBOLS:
            cands = cands_by_symbol[symbol]
            pick, reason = _pick_best_with_reason(cands, zmin, zmax, cur, now_ts)
            local_zmin = zmin
            local_zmax = zmax
            if pick is None and reason == "out_of_zone":
                reason = "out_of_zone_after_expand"
                for _ in range(int(CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS)):
                    local_zmin = max(0.0, local_zmin - float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR))
                    local_zmax = local_zmax + float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR)
                    pick, r2 = _pick_best_with_reason(cands, local_zmin, local_zmax, cur, now_ts)
                    if pick is not None:
                        reason = "picked_after_expand"
                        break
                    reason = r2 if r2 != "out_of_zone" else "out_of_zone_after_expand"
            strongest = None
            if cands:
                strongest = sorted(
                    cands, key=lambda x: (-x.volume_peak, -x.strength, x.distance_atr, -x.updated_at)
                )[0]
            rows.append(
                {
                    "symbol": symbol,
                    "direction": direction,
                    "zone_min_atr": zmin,
                    "zone_max_atr": zmax,
                    "zone_used_min_atr": local_zmin,
                    "zone_used_max_atr": local_zmax,
                    "candidates_n": len(cands),
                    "strongest_price": strongest.price if strongest else None,
                    "strongest_distance_atr": strongest.distance_atr if strongest else None,
                    "strongest_volume_peak": strongest.volume_peak if strongest else None,
                    "picked": 1 if pick is not None else 0,
                    "picked_price": pick.price if pick else None,
                    "picked_distance_atr": pick.distance_atr if pick else None,
                    "picked_volume_peak": pick.volume_peak if pick else None,
                    "picked_level_type": pick.level_type if pick else None,
                    "picked_source_is_manual_global": pick.source_is_manual_global if pick else None,
                    "reason": reason,
                    "ref_price": (live_prices.get(symbol).price if live_prices.get(symbol) else None),
                    "ref_price_source": (live_prices.get(symbol).source if live_prices.get(symbol) else "db_1m_close"),
                }
            )
    conn.close()
    return pd.DataFrame(rows)


def build_cycle_levels_candidates_df():
    # init_db() и run_migrations() вызываются в supervisor при старте
    # Не вызывать в каждом тике - создаёт database locked
    now_ts = _now_ts()
    conn = get_connection()
    cur = conn.cursor()
    feed = get_price_feed()
    live_prices: Dict[str, PricePoint] = feed.get_prices(TRADING_SYMBOLS)
    rows: List[dict] = []
    for direction in ("long", "short"):
        cands_by_symbol: Dict[str, List[Candidate]] = {}
        for symbol in TRADING_SYMBOLS:
            atr = _atr_daily(symbol, cur)
            if atr is None:
                cands_by_symbol[symbol] = []
                continue
            pp = live_prices.get(symbol)
            if pp is None:
                px = _latest_close_1m(symbol, cur)
                if px is None:
                    cands_by_symbol[symbol] = []
                    continue
                pp = PricePoint(price=float(px), ts=now_ts, source="db_1m_close")
            cands_by_symbol[symbol] = _load_candidates_with_ref_price(
                symbol, direction, pp.price, atr, cur
            )
        zmin, zmax = _direction_zone(cands_by_symbol)
        for symbol in TRADING_SYMBOLS:
            local_zmin = zmin
            local_zmax = zmax
            if int(CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS) > 0:
                local_zmin = max(
                    0.0,
                    zmin - float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR) * int(CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS),
                )
                local_zmax = zmax + float(CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR) * int(CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS)
            for c in cands_by_symbol[symbol]:
                on_cd = _is_on_cooldown(c, cur, now_ts)
                in_base_zone = c.distance_atr >= zmin and c.distance_atr <= zmax
                in_expanded_zone = c.distance_atr >= local_zmin and c.distance_atr <= local_zmax
                pass_min_dist = c.distance_atr >= float(CYCLE_LEVELS_MIN_DIST_ATR)
                eligible = pass_min_dist and (not on_cd) and in_expanded_zone
                rows.append(
                    {
                        "symbol": c.symbol,
                        "direction": c.direction,
                        "price": c.price,
                        "distance_atr": c.distance_atr,
                        "volume_peak": c.volume_peak,
                        "strength": c.strength,
                        "tier": c.tier,
                        "level_type": c.level_type,
                        "source_is_manual_global": c.source_is_manual_global,
                        "source_level_id": c.id,
                        "updated_at": c.updated_at,
                        "min_dist_cfg": float(CYCLE_LEVELS_MIN_DIST_ATR),
                        "zone_min_atr": zmin,
                        "zone_max_atr": zmax,
                        "zone_min_after_expand_atr": local_zmin,
                        "zone_max_after_expand_atr": local_zmax,
                        "pass_min_dist": 1 if pass_min_dist else 0,
                        "on_cooldown": 1 if on_cd else 0,
                        "in_base_zone": 1 if in_base_zone else 0,
                        "in_expanded_zone": 1 if in_expanded_zone else 0,
                        "eligible_for_pick": 1 if eligible else 0,
                        "ref_price": (live_prices.get(symbol).price if live_prices.get(symbol) else None),
                        "ref_price_source": (live_prices.get(symbol).source if live_prices.get(symbol) else "db_1m_close"),
                    }
                )
    conn.close()
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(
            ["symbol", "direction", "eligible_for_pick", "volume_peak", "strength", "distance_atr", "updated_at"],
            ascending=[True, True, False, False, False, True, False],
        ).reset_index(drop=True)
    return df


def fetch_cycle_levels_df():
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            cl.cycle_id, cl.symbol, cl.direction, cl.level_step, cl.level_price,
            cl.distance_atr, cl.tier, cl.volume_peak, pl.level_type AS source_level_type,
            pl.strength AS source_strength, cl.ref_price, cl.ref_price_source, cl.ref_price_ts,
            cl.is_primary, cl.is_active,
            cl.frozen_at, cl.updated_at, ts.position_state, ts.cycle_phase
        FROM cycle_levels cl
        LEFT JOIN price_levels pl ON pl.id = cl.source_level_id
        LEFT JOIN trading_state ts ON ts.id = 1
        ORDER BY cl.symbol, cl.direction
        """,
        conn,
    )
    if not df.empty:
        px = pd.read_sql_query(
            """
            SELECT o.symbol, o.close AS current_price
            FROM ohlcv o
            JOIN (
                SELECT symbol, MAX(timestamp) AS mx
                FROM ohlcv
                WHERE timeframe = '1m'
                GROUP BY symbol
            ) m ON m.symbol = o.symbol AND m.mx = o.timestamp
            WHERE o.timeframe = '1m'
            """,
            conn,
        )
        if not px.empty:
            df = df.merge(px, on="symbol", how="left")
    conn.close()
    return df


def export_cycle_levels_sheets_snapshot() -> Dict[str, int]:
    """Экспорт cycle_levels в Google Sheets с обработкой ошибок database locked."""
    import sqlite3
    
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    spreadsheet_title = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
    spreadsheet_url = os.getenv("MARKET_AUDIT_SHEET_URL")
    spreadsheet_id = os.getenv("MARKET_AUDIT_SHEET_ID")
    exporter = SheetsExporter(
        credentials_path=credentials_path,
        spreadsheet_title=spreadsheet_title,
        spreadsheet_url=spreadsheet_url,
        spreadsheet_id=spreadsheet_id,
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    
    try:
        df = fetch_cycle_levels_df()
        if df.empty:
            df = df.assign(note="cycle_levels is empty")
        df["exported_at_utc"] = now_iso
        exporter.export_dataframe_to_sheet(df, spreadsheet_title, CYCLE_LEVELS_WORKSHEET)
        cycle_levels_rows = len(df)
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logger.warning("export_cycle_levels_sheets_snapshot: database locked, skipping cycle_levels export")
            cycle_levels_rows = 0
        else:
            raise
    
    try:
        diag = build_cycle_levels_diagnostics()
        diag["exported_at_utc"] = now_iso
        exporter.export_dataframe_to_sheet(diag, spreadsheet_title, CYCLE_LEVELS_DIAG_WORKSHEET)
        diag_rows = len(diag)
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logger.warning("export_cycle_levels_sheets_snapshot: database locked, skipping diag export")
            diag_rows = 0
        else:
            raise
    
    try:
        cands = build_cycle_levels_candidates_df()
        cands["exported_at_utc"] = now_iso
        exporter.export_dataframe_to_sheet(cands, spreadsheet_title, CYCLE_LEVELS_CANDIDATES_WORKSHEET)
        candidates_rows = len(cands)
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logger.warning("export_cycle_levels_sheets_snapshot: database locked, skipping candidates export")
            candidates_rows = 0
        else:
            raise
    
    return {"cycle_levels_rows": cycle_levels_rows, "diag_rows": diag_rows, "candidates_rows": candidates_rows}


# ========== НОВАЯ ФУНКЦИЯ: экспорт всех активных горизонтальных уровней ==========
def export_all_active_levels_to_sheets() -> Dict[str, int]:
    """Выгружает все активные уровни из price_levels (is_active=1, status='active')."""
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    spreadsheet_title = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
    spreadsheet_url = os.getenv("MARKET_AUDIT_SHEET_URL")
    spreadsheet_id = os.getenv("MARKET_AUDIT_SHEET_ID")
    exporter = SheetsExporter(
        credentials_path=credentials_path,
        spreadsheet_title=spreadsheet_title,
        spreadsheet_url=spreadsheet_url,
        spreadsheet_id=spreadsheet_id,
    )
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            symbol, price, level_type, layer, strength, volume_peak,
            duration_hours, tier, created_at, updated_at, is_active, status,
            stable_level_id, origin, timeframe
        FROM price_levels
        WHERE is_active = 1 AND status = 'active'
        ORDER BY symbol, price
        """,
        conn,
    )
    conn.close()
    now_iso = datetime.now(timezone.utc).isoformat()
    df["exported_at_utc"] = now_iso
    ws_name = os.getenv("ALL_ACTIVE_LEVELS_WORKSHEET", "all_active_levels")
    exporter.export_dataframe_to_sheet(df, spreadsheet_title, ws_name)
    return {"rows": len(df), "worksheet": ws_name}