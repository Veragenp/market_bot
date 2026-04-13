"""
Пересчет Volume Profile Peaks (HVN) и сохранение в SQLite (`price_levels`) для контроля.

Окно расчёта:
  - Если заданы env `PRO_LEVELS_LOOKBACK_DAYS` и/или `PRO_LEVELS_LOOKBACK_HOURS`:
      окно = [last_1m_timestamp_in_db - lookback_seconds, last_1m_timestamp_in_db]
      (якорь = последняя 1m свеча в БД).
  - Перед find_pro_levels: метрика качества 1m OHLC; при «плоских» свечах — ресемпл 5m
      в памяти (см. `analytics/vp_ohlc_source.py`, пороги `VP_OHLC_*` в settings).
  - Иначе:
      используется календарный месяц `DYNAMIC_ZONES_YEAR` / `DYNAMIC_ZONES_MONTH`
      (fallback как в `export_to_sheets.py`).

Планировщик: `VP_LOCAL_REBUILD_INTERVAL_HOURS` (по умолчанию 4) в `trading_bot.config.settings`.
Пустой find_pro_levels: при `VP_LOCAL_CLEAR_ON_EMPTY_REBUILD=1` активные vp_local по символу архивируются.

Сохранение в БД:
  - `price_levels.level_type` = `vp_local` / `volume_profile_peaks` (константа в коде)
  - `price_levels.is_active`:
      перед вставкой новых строк деактивируем старые активные записи этого `symbol`+`level_type`
      (история остается).
  - `price_levels.layer`:
      `volpeak_{days}d_{hours}h_{start_ts}_{end_ts}` и при 5m суффикс `_5mrs`
  - `price_levels.timeframe`: `1m` или `5m` (5m = ресемпл из 1m)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

MARKET_BOT_ROOT = Path(__file__).resolve().parents[2]
if str(MARKET_BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(MARKET_BOT_ROOT))

from config import (
    TRADING_SYMBOLS,
    PRO_LEVELS_LOOKBACK_DAYS,
    PRO_LEVELS_LOOKBACK_HOURS,
    PRO_LEVELS_HEIGHT_MULT,
    PRO_LEVELS_DISTANCE_PCT,
    PRO_LEVELS_VALLEY_THRESHOLD,
    PRO_LEVELS_MIN_DURATION_HOURS,
    PRO_LEVELS_MAX_LEVELS,
    PRO_LEVELS_INCLUDE_ALL_TIERS,
    PRO_LEVELS_FINAL_MERGE_PCT,
    PRO_LEVELS_VALLEY_MERGE_THRESHOLD,
    PRO_LEVELS_ENABLE_VALLEY_MERGE,
    PRO_LEVELS_DEDUP_ROUND_PCT,
    PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD,
    PRO_LEVELS_LEGACY_WEAK_MERGE,
    PRO_LEVELS_RUN_SOFT_PASS,
    PRO_LEVELS_STRICT_HEIGHT_WEAK,
    PRO_LEVELS_STRICT_HEIGHT_MULT,
    PRO_LEVELS_SOFT_HEIGHT_STRONG,
    PRO_LEVELS_SOFT_HEIGHT_WEAK,
    PRO_LEVELS_SOFT_HEIGHT_MULT,
    PRO_LEVELS_SOFT_FINAL_MERGE_PCT,
    PRO_LEVELS_EXCLUDE_RESERVED_PCT,
    PRO_LEVELS_WEAK_MIN_DURATION,
    VP_LOCAL_CLEAR_ON_EMPTY_REBUILD,
)
from trading_bot.analytics.dynamic_accumulation_zones import slice_calendar_month_utc
from trading_bot.analytics.vp_ohlc_source import select_vp_ohlcv_dataframe
from trading_bot.analytics.volume_profile_peaks import find_pro_levels, get_adaptive_params
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_TYPE_VOLUME_PROFILE_PEAKS,
    save_volume_profile_peaks_levels_to_db,
)


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _active_vp_local_count(symbol: str) -> int:
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT COUNT(*)
        FROM price_levels
        WHERE symbol = ? AND level_type = ? AND is_active = 1 AND status = 'active'
        """,
        (symbol, LEVEL_TYPE_VOLUME_PROFILE_PEAKS),
    ).fetchone()
    conn.close()
    return int(row[0] if row else 0)


def _build_params(work: pd.DataFrame, symbol: str, overrides: dict | None = None) -> dict:
    common = _compute_find_pro_params(work, symbol)
    if overrides:
        for key, value in overrides.items():
            if value is not None:
                common[key] = value
    return common


def _default_prev_calendar_month_utc() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _get_last_ts_1m(symbol: str) -> Optional[int]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(timestamp)
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        """,
        (symbol,),
    )
    row = cur.fetchone()
    conn.close()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _fetch_ohlcv_1m_range(symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m' AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
        """,
        conn,
        params=(symbol, int(start_ts), int(end_ts)),
    )
    conn.close()
    return df


def _fetch_ohlcv_1m_all_for_symbol(symbol: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        ORDER BY timestamp
        """,
        conn,
        params=(symbol,),
    )
    conn.close()
    return df


def _compute_find_pro_params(work: pd.DataFrame, symbol: str) -> dict:
    """
    Параметры `find_pro_levels`, максимально повторяющие блок в `export_to_sheets.py`.
    """
    params = get_adaptive_params(work, symbol=symbol)

    height_mult = float(PRO_LEVELS_HEIGHT_MULT) if PRO_LEVELS_HEIGHT_MULT is not None else None
    distance_pct = (
        float(PRO_LEVELS_DISTANCE_PCT) if PRO_LEVELS_DISTANCE_PCT is not None else float(params["distance_pct"])
    )
    valley_threshold = (
        float(PRO_LEVELS_VALLEY_THRESHOLD)
        if PRO_LEVELS_VALLEY_THRESHOLD is not None
        else float(params["valley_threshold"])
    )

    tick_size = float(params["tick_size"])
    top_n = int(params.get("top_n", 10))

    min_duration_hours = (
        float(PRO_LEVELS_MIN_DURATION_HOURS)
        if PRO_LEVELS_MIN_DURATION_HOURS is not None
        else float(params.get("min_duration_hours", 6.0))
    )

    max_levels: int | None = params.get("max_levels")
    if PRO_LEVELS_MAX_LEVELS is not None:
        max_levels = int(PRO_LEVELS_MAX_LEVELS)

    include_all_tiers = True if PRO_LEVELS_INCLUDE_ALL_TIERS is None else bool(PRO_LEVELS_INCLUDE_ALL_TIERS)

    final_merge_pct: float | None = params.get("dynamic_merge_pct")
    if PRO_LEVELS_FINAL_MERGE_PCT is not None:
        final_merge_pct = float(PRO_LEVELS_FINAL_MERGE_PCT)

    valley_merge_threshold = (
        float(PRO_LEVELS_VALLEY_MERGE_THRESHOLD)
        if PRO_LEVELS_VALLEY_MERGE_THRESHOLD is not None
        else float(params.get("valley_merge_threshold", 0.5))
    )
    enable_valley_merge = bool(PRO_LEVELS_ENABLE_VALLEY_MERGE)

    dedup_round_pct = (
        float(PRO_LEVELS_DEDUP_ROUND_PCT)
        if PRO_LEVELS_DEDUP_ROUND_PCT is not None
        else float(params.get("dedup_round_pct", 0.001))
    )

    final_merge_valley_threshold: float | None = (
        float(PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD)
        if PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD is not None
        else params.get("final_merge_valley_threshold")
    )

    legacy_weak_merge = bool(PRO_LEVELS_LEGACY_WEAK_MERGE)
    run_soft_pass = bool(PRO_LEVELS_RUN_SOFT_PASS)

    strict_height_percentile_weak: float | None = (
        float(PRO_LEVELS_STRICT_HEIGHT_WEAK) if PRO_LEVELS_STRICT_HEIGHT_WEAK is not None else None
    )
    strict_height_mult: float | None = (
        float(PRO_LEVELS_STRICT_HEIGHT_MULT) if PRO_LEVELS_STRICT_HEIGHT_MULT is not None else None
    )

    soft_height_percentile_strong = (
        float(PRO_LEVELS_SOFT_HEIGHT_STRONG) if PRO_LEVELS_SOFT_HEIGHT_STRONG is not None else 0.6
    )
    soft_height_percentile_weak = (
        float(PRO_LEVELS_SOFT_HEIGHT_WEAK) if PRO_LEVELS_SOFT_HEIGHT_WEAK is not None else 0.55
    )
    soft_height_mult: float | None = (
        float(PRO_LEVELS_SOFT_HEIGHT_MULT) if PRO_LEVELS_SOFT_HEIGHT_MULT is not None else None
    )
    soft_final_merge_pct: float | None = (
        float(PRO_LEVELS_SOFT_FINAL_MERGE_PCT) if PRO_LEVELS_SOFT_FINAL_MERGE_PCT is not None else None
    )
    exclude_reserved_pct: float | None = (
        float(PRO_LEVELS_EXCLUDE_RESERVED_PCT) if PRO_LEVELS_EXCLUDE_RESERVED_PCT is not None else None
    )

    soft_min_duration_hours = (
        float(PRO_LEVELS_WEAK_MIN_DURATION) if PRO_LEVELS_WEAK_MIN_DURATION is not None else 4.0
    )

    height_percentile_strong = float(params.get("height_percentile_strong", 0.85))
    height_percentile_weak = float(params.get("height_percentile_weak", 0.65))

    two_pass_mode = not legacy_weak_merge
    return {
        "height_mult": height_mult,
        "distance_pct": distance_pct,
        "valley_threshold": valley_threshold,
        "tick_size": tick_size,
        "top_n": top_n,
        "min_duration_hours": min_duration_hours,
        "max_levels": max_levels,
        "include_all_tiers": include_all_tiers,
        "final_merge_pct": final_merge_pct,
        "valley_merge_threshold": valley_merge_threshold,
        "enable_valley_merge": enable_valley_merge,
        "allow_stage_b_overlap": True,
        "dedup_round_pct": dedup_round_pct,
        "final_merge_valley_threshold": final_merge_valley_threshold,
        "legacy_weak_merge": legacy_weak_merge,
        "two_pass_mode": two_pass_mode,
        "run_soft_pass": run_soft_pass,
        "height_percentile_strong": height_percentile_strong,
        "height_percentile_weak": height_percentile_weak,
        "strict_height_percentile_weak": strict_height_percentile_weak,
        "strict_height_mult": strict_height_mult,
        "exclude_reserved_pct": exclude_reserved_pct,
        "soft_height_percentile_strong": soft_height_percentile_strong,
        "soft_height_percentile_weak": soft_height_percentile_weak,
        "soft_height_mult": soft_height_mult,
        "soft_min_duration_hours": soft_min_duration_hours,
        "soft_final_merge_pct": soft_final_merge_pct,
    }


def _run_for_symbols(
    symbols: list[str],
    *,
    min_levels_per_symbol: int,
    pass_name: str,
    param_overrides: dict | None = None,
    archive_on_empty: bool,
    keep_if_worse: bool = False,
    append_only: bool = False,
) -> list[str]:
    lookback_days = PRO_LEVELS_LOOKBACK_DAYS
    lookback_hours = PRO_LEVELS_LOOKBACK_HOURS
    use_lookback = (lookback_days is not None) or (lookback_hours is not None)

    if use_lookback:
        lookback_days = int(lookback_days or 0)
        lookback_hours = int(lookback_hours or 0)
        lookback_seconds = lookback_days * 86400 + lookback_hours * 3600
    else:
        year, month = _default_prev_calendar_month_utc()
        lookback_seconds = None

    for symbol in symbols:
        if use_lookback:
            end_ts = _get_last_ts_1m(symbol)
            if end_ts is None:
                print(f"SKIP {symbol}: no 1m candles in DB (anchor ts missing)")
                continue
            start_ts = end_ts - int(lookback_seconds)
            df = _fetch_ohlcv_1m_range(symbol, start_ts, end_ts)
            if df.empty:
                print(f"SKIP {symbol}: empty 1m data in lookback window {start_ts}->{end_ts}")
                continue
            base_layer = f"volpeak_{lookback_days}d_{lookback_hours}h_{start_ts}_{end_ts}"
        else:
            df_all = _fetch_ohlcv_1m_all_for_symbol(symbol)
            work = slice_calendar_month_utc(df_all, year, month)
            if work.empty:
                print(f"SKIP {symbol}: no 1m data for calendar month {year}-{month:02d}")
                continue
            start_ts = int(work["timestamp"].min())
            end_ts = int(work["timestamp"].max())
            seconds = max(0, end_ts - start_ts)
            days = seconds // 86400
            hours = (seconds % 86400) // 3600
            base_layer = f"volpeak_{int(days)}d_{int(hours)}h_{start_ts}_{end_ts}"
            df = work

        if df.empty:
            print(f"SKIP {symbol}: empty OHLCV window")
            continue

        df_vp, tf_used, vp_diag = select_vp_ohlcv_dataframe(df)
        layer = f"{base_layer}_5mrs" if tf_used == "5m" else base_layer

        common = _build_params(df_vp, symbol, overrides=param_overrides)
        final_levels = find_pro_levels(df_vp, symbol=symbol, **common)
        if final_levels is None or final_levels.empty:
            if archive_on_empty and VP_LOCAL_CLEAR_ON_EMPTY_REBUILD:
                save_volume_profile_peaks_levels_to_db(
                    symbol,
                    pd.DataFrame(),
                    layer=f"{layer}_no_levels",
                    level_type=LEVEL_TYPE_VOLUME_PROFILE_PEAKS,
                    timeframe=tf_used,
                    archive_active_when_empty=True,
                )
                print(
                    f"SKIP {symbol}: find_pro_levels() empty — активные vp_local сняты "
                    f"(VP_LOCAL_CLEAR_ON_EMPTY_REBUILD)"
                )
            else:
                print(
                    f"SKIP {symbol}: find_pro_levels() empty — активные vp_local не трогаем "
                    f"({pass_name})"
                )
            continue

        old_count = _active_vp_local_count(symbol)
        new_count = len(final_levels)
        if keep_if_worse and old_count > 0 and new_count < old_count:
            print(
                f"SKIP {symbol}: keep previous vp_local (old={old_count} > new={new_count}) "
                f"({pass_name})"
            )
            continue

        save_volume_profile_peaks_levels_to_db(
            symbol,
            final_levels,
            layer=layer,
            level_type=LEVEL_TYPE_VOLUME_PROFILE_PEAKS,
            timeframe=tf_used,
            append_only=append_only,
        )

        src = vp_diag.get("vp_source", "")
        q1 = vp_diag.get("quality_1m") or {}
        print(
            f"OK[{pass_name}]: {symbol} layer={layer} tf={tf_used} vp_src={src} "
            f"1m_flat={q1.get('flat_frac')} 1m_mrng={q1.get('median_rel_range')} "
            f"final={0 if final_levels is None else len(final_levels)}"
        )

    low_symbols: list[str] = []
    for symbol in symbols:
        cnt = _active_vp_local_count(symbol)
        if cnt < min_levels_per_symbol:
            low_symbols.append(symbol)
    return low_symbols


def main() -> None:
    init_db()
    min_levels = _env_int("VP_LOCAL_MIN_LEVELS_PER_SYMBOL", 4)
    run_refine_passes = _env_bool("VP_LOCAL_ENABLE_REFINE_PASSES", True)

    symbols = list(TRADING_SYMBOLS)
    print(f"VP_LOCAL pass#1 (base) symbols={len(symbols)} min_levels={min_levels}")
    low = _run_for_symbols(
        symbols,
        min_levels_per_symbol=min_levels,
        pass_name="base",
        param_overrides=None,
        archive_on_empty=not run_refine_passes,
    )
    print(f"VP_LOCAL after pass#1: below_min={len(low)}")

    if not run_refine_passes or not low:
        return

    pass2_overrides = {
        "max_levels": _env_int("VP_LOCAL_REFINE_PASS2_MAX_LEVELS", 28),
        "final_merge_pct": _env_float("VP_LOCAL_REFINE_PASS2_FINAL_MERGE_PCT", 0.0015),
        "enable_valley_merge": _env_bool("VP_LOCAL_REFINE_PASS2_ENABLE_VALLEY_MERGE", False),
        "exclude_reserved_pct": _env_float("VP_LOCAL_REFINE_PASS2_EXCLUDE_RESERVED_PCT", 0.0),
        "run_soft_pass": _env_bool("VP_LOCAL_REFINE_PASS2_RUN_SOFT_PASS", True),
    }
    print(f"VP_LOCAL pass#2 (refine-soft) symbols={len(low)}")
    low = _run_for_symbols(
        low,
        min_levels_per_symbol=min_levels,
        pass_name="refine_soft",
        param_overrides=pass2_overrides,
        archive_on_empty=False,
        keep_if_worse=True,
        append_only=True,
    )
    print(f"VP_LOCAL after pass#2: below_min={len(low)}")
    if not low:
        return

    pass3_overrides = {
        "max_levels": _env_int("VP_LOCAL_REFINE_PASS3_MAX_LEVELS", 40),
        "final_merge_pct": _env_float("VP_LOCAL_REFINE_PASS3_FINAL_MERGE_PCT", 0.0010),
        "enable_valley_merge": _env_bool("VP_LOCAL_REFINE_PASS3_ENABLE_VALLEY_MERGE", False),
        "exclude_reserved_pct": _env_float("VP_LOCAL_REFINE_PASS3_EXCLUDE_RESERVED_PCT", 0.0),
        "run_soft_pass": _env_bool("VP_LOCAL_REFINE_PASS3_RUN_SOFT_PASS", True),
        "distance_pct": _env_float("VP_LOCAL_REFINE_PASS3_DISTANCE_PCT", 0.20),
    }
    print(f"VP_LOCAL pass#3 (refine-ultra) symbols={len(low)}")
    low = _run_for_symbols(
        low,
        min_levels_per_symbol=min_levels,
        pass_name="refine_ultra",
        param_overrides=pass3_overrides,
        archive_on_empty=False,
        keep_if_worse=True,
        append_only=True,
    )
    print(f"VP_LOCAL after pass#3: below_min={len(low)}")
    if low:
        print("VP_LOCAL unresolved symbols:", ",".join(low))


if __name__ == "__main__":
    main()

