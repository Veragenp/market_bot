from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import (
    ANALYTIC_SYMBOLS,
    FILL_MISSING_WEEKENDS,
    SOURCE_BINANCE,
    TRADING_SYMBOLS,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db
from trading_bot.analytics.dynamic_accumulation_zones import (
    DEFAULT_CLUSTER_MERGE_MAX_GAP_PCT,
    DEFAULT_CLUSTER_MERGE_MAX_TIME_GAP_HOURS,
    DEFAULT_CLUSTER_THRESHOLD_PCT,
    DEFAULT_POC_MERGE_THRESHOLD_PCT,
    DEFAULT_PRICE_BAND_TICK_MULTIPLIER,
    run_pipeline,
    slice_calendar_month_utc,
)
from trading_bot.analytics.volume_profile_peaks import (
    find_pro_levels,
    get_adaptive_params,
)
from trading_bot.data.repositories import get_ohlcv_filled
from trading_bot.tools.sheets_exporter import SheetsExporter

SHEET_TITLE = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_URL = os.getenv("MARKET_AUDIT_SHEET_URL")
SHEET_ID = os.getenv("MARKET_AUDIT_SHEET_ID")
DYNAMIC_ZONES_SYMBOL = os.getenv("DYNAMIC_ZONES_SYMBOL", "BTC/USDT")
# Список пар для листов volume profile (через запятую); если пусто — три пары по умолчанию.
VOLUME_PEAK_SYMBOLS = os.getenv("VOLUME_PEAK_SYMBOLS")
DEFAULT_VOLUME_PEAK_SYMBOLS = "BTC/USDT,SOL/USDT,AAVE/USDT"


def resolve_volume_peak_export_symbols() -> str:
    if VOLUME_PEAK_SYMBOLS and str(VOLUME_PEAK_SYMBOLS).strip():
        return str(VOLUME_PEAK_SYMBOLS).strip()
    return DEFAULT_VOLUME_PEAK_SYMBOLS
DYNAMIC_ZONES_YEAR = os.getenv("DYNAMIC_ZONES_YEAR")
DYNAMIC_ZONES_MONTH = os.getenv("DYNAMIC_ZONES_MONTH")
DYNAMIC_ZONES_BIN_STEP_USDT = os.getenv("DYNAMIC_ZONES_BIN_STEP_USDT")
DYNAMIC_ZONES_POC_THRESHOLD_PCT = os.getenv("DYNAMIC_ZONES_POC_THRESHOLD_PCT")
DYNAMIC_ZONES_CLUSTER_PCT = os.getenv("DYNAMIC_ZONES_CLUSTER_PCT")
DYNAMIC_ZONES_TOP_N_PER_BAND = os.getenv("DYNAMIC_ZONES_TOP_N_PER_BAND")
DYNAMIC_ZONES_PRICE_BAND_USDT = os.getenv("DYNAMIC_ZONES_PRICE_BAND_USDT")
DYNAMIC_ZONES_CLUSTER_MERGE_GAP_PCT = os.getenv("DYNAMIC_ZONES_CLUSTER_MERGE_GAP_PCT")
DYNAMIC_ZONES_CLUSTER_MERGE_TIME_GAP_HOURS = os.getenv("DYNAMIC_ZONES_CLUSTER_MERGE_TIME_GAP_HOURS")
# Совместимость со старым именем (коридор цены для пост-склейки)
DYNAMIC_ZONES_WEIGHTED_MERGE_PCT = os.getenv("DYNAMIC_ZONES_WEIGHTED_MERGE_PCT")
# Вкладка с пиками профиля объёма (старое имя DBSCAN_ZONES_WORKSHEET сохраняем для совместимости)
VOLUME_PEAK_LEVELS_WORKSHEET = os.getenv(
    "VOLUME_PEAK_LEVELS_WORKSHEET"
) or os.getenv("DBSCAN_ZONES_WORKSHEET", "dynamic_accumulation_zones_dbscan")
VOLUME_PEAK_ANALYSIS_WORKSHEET = os.getenv(
    "VOLUME_PEAK_ANALYSIS_WORKSHEET", "volume_profile_peaks_analysis"
)
PRO_LEVELS_HEIGHT_MULT = os.getenv("PRO_LEVELS_HEIGHT_MULT")
PRO_LEVELS_DISTANCE_PCT = os.getenv("PRO_LEVELS_DISTANCE_PCT")
PRO_LEVELS_VALLEY_THRESHOLD = os.getenv("PRO_LEVELS_VALLEY_THRESHOLD")
PRO_LEVELS_MIN_DURATION_HOURS = os.getenv("PRO_LEVELS_MIN_DURATION_HOURS")
PRO_LEVELS_MAX_LEVELS = os.getenv("PRO_LEVELS_MAX_LEVELS")
PRO_LEVELS_INCLUDE_ALL_TIERS = os.getenv("PRO_LEVELS_INCLUDE_ALL_TIERS")
PRO_LEVELS_FINAL_MERGE_PCT = os.getenv("PRO_LEVELS_FINAL_MERGE_PCT")
PRO_LEVELS_VALLEY_MERGE_THRESHOLD = os.getenv("PRO_LEVELS_VALLEY_MERGE_THRESHOLD")
PRO_LEVELS_ENABLE_VALLEY_MERGE = os.getenv("PRO_LEVELS_ENABLE_VALLEY_MERGE", "true")
PRO_LEVELS_DEDUP_ROUND_PCT = os.getenv("PRO_LEVELS_DEDUP_ROUND_PCT")
PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD = os.getenv("PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD")
PRO_LEVELS_LEGACY_WEAK_MERGE = os.getenv("PRO_LEVELS_LEGACY_WEAK_MERGE", "false")
PRO_LEVELS_RUN_SOFT_PASS = os.getenv("PRO_LEVELS_RUN_SOFT_PASS", "true")
PRO_LEVELS_STRICT_HEIGHT_WEAK = os.getenv("PRO_LEVELS_STRICT_HEIGHT_WEAK")
PRO_LEVELS_STRICT_HEIGHT_MULT = os.getenv("PRO_LEVELS_STRICT_HEIGHT_MULT")
PRO_LEVELS_SOFT_HEIGHT_STRONG = os.getenv("PRO_LEVELS_SOFT_HEIGHT_STRONG")
PRO_LEVELS_SOFT_HEIGHT_WEAK = os.getenv("PRO_LEVELS_SOFT_HEIGHT_WEAK")
PRO_LEVELS_SOFT_HEIGHT_MULT = os.getenv("PRO_LEVELS_SOFT_HEIGHT_MULT")
PRO_LEVELS_SOFT_FINAL_MERGE_PCT = os.getenv("PRO_LEVELS_SOFT_FINAL_MERGE_PCT")
PRO_LEVELS_EXCLUDE_RESERVED_PCT = os.getenv("PRO_LEVELS_EXCLUDE_RESERVED_PCT")
PRO_LEVELS_WEAK_MIN_DURATION = os.getenv("PRO_LEVELS_WEAK_MIN_DURATION")


def _ts_to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def _parse_cluster_merge_time_gap_hours(raw: str | None) -> float | None:
    """Пусто → дефолт из модуля; none/off → без лимита разрыва по времени."""
    if raw is None or str(raw).strip() == "":
        return float(DEFAULT_CLUSTER_MERGE_MAX_TIME_GAP_HOURS)
    s = str(raw).strip().lower()
    if s in ("none", "off", "false", "-1"):
        return None
    return float(s)


def _fetch_ohlcv_sample(
    symbols: Iterable[str],
    timeframes: Iterable[str],
    limit: int,
    fill_weekends: bool = False,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        for timeframe in timeframes:
            if fill_weekends and timeframe == "1d":
                candles = get_ohlcv_filled(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    fill_weekends=True,
                )
            else:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
                    FROM ohlcv
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (symbol, timeframe, limit),
                )
                candles = [dict(r) for r in cur.fetchall()]
                conn.close()
                candles.reverse()

            for c in candles:
                is_synthetic_fill = fill_weekends and timeframe == "1d" and c.get("open") is None
                rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp_utc": _ts_to_iso_utc(int(c["timestamp"])),
                        "open": c.get("open"),
                        "high": c.get("high"),
                        "low": c.get("low"),
                        "close": None if is_synthetic_fill else c.get("close"),
                        "close_filled": c.get("close"),
                        "volume": c.get("volume"),
                        "extra": c.get("extra"),
                        "source": c.get("source") or SOURCE_BINANCE,
                        "updated_at": c.get("updated_at"),
                    }
                )
    return pd.DataFrame(rows)


def _fetch_coinglass_sample(limit: int = 50) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    conn = get_connection()
    cur = conn.cursor()

    for symbol in TRADING_SYMBOLS:
        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, long_volume, short_volume, total_volume, updated_at
            FROM liquidations
            WHERE symbol = ? AND timeframe = '4h'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        liq = [dict(r) for r in cur.fetchall()]
        liq.reverse()
        for r in liq:
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": "4h",
                    "timestamp_utc": _ts_to_iso_utc(int(r["timestamp"])),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": r.get("total_volume"),
                    "extra": json.dumps(
                        {
                            "long_volume": r.get("long_volume"),
                            "short_volume": r.get("short_volume"),
                        },
                        ensure_ascii=True,
                    ),
                    "source": r.get("exchange") or SOURCE_BINANCE,
                    "updated_at": r.get("updated_at"),
                }
            )

        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, oi_value, oi_change_24h, updated_at
            FROM open_interest
            WHERE symbol = ? AND timeframe = '4h'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        oi = [dict(r) for r in cur.fetchall()]
        oi.reverse()
        for r in oi:
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": "4h",
                    "timestamp_utc": _ts_to_iso_utc(int(r["timestamp"])),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": r.get("oi_value"),
                    "volume": None,
                    "extra": json.dumps(
                        {"oi_change_24h": r.get("oi_change_24h")},
                        ensure_ascii=True,
                    ),
                    "source": r.get("exchange") or SOURCE_BINANCE,
                    "updated_at": r.get("updated_at"),
                }
            )
    conn.close()
    return pd.DataFrame(rows)


def _build_audit_log(entries: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(entries, columns=["source", "worksheet", "rows", "last_exported_at_utc"])


def _fetch_indices_agg_sample(limit: int = 200) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    symbols = ["TOTAL", "TOTAL2", "TOTAL3", "BTCD", "OTHERS", "OTHERSD"]
    conn = get_connection()
    cur = conn.cursor()
    for symbol in symbols:
        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
            FROM ohlcv
            WHERE symbol = ? AND source = 'coingecko_agg'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        data = [dict(r) for r in cur.fetchall()]
        data.reverse()
        for c in data:
            rows.append(
                {
                    "symbol": c.get("symbol"),
                    "timeframe": c.get("timeframe"),
                    "timestamp_utc": _ts_to_iso_utc(int(c["timestamp"])),
                    "open": c.get("open"),
                    "high": c.get("high"),
                    "low": c.get("low"),
                    "close": c.get("close"),
                    "close_filled": c.get("close"),
                    "volume": c.get("volume"),
                    "extra": c.get("extra"),
                    "source": c.get("source"),
                    "updated_at": c.get("updated_at"),
                }
            )
    conn.close()
    return pd.DataFrame(rows)


def _fetch_all_coingecko_agg() -> pd.DataFrame:
    """Every row in ohlcv with source=coingecko_agg (full dump for validation)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
        FROM ohlcv
        WHERE source = 'coingecko_agg'
        ORDER BY symbol, timeframe, timestamp
        """
    )
    rows: List[Dict[str, Any]] = []
    for c in cur.fetchall():
        c = dict(c)
        rows.append(
            {
                "symbol": c.get("symbol"),
                "timeframe": c.get("timeframe"),
                "timestamp_utc": _ts_to_iso_utc(int(c["timestamp"])),
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume"),
                "extra": c.get("extra"),
                "source": c.get("source"),
                "updated_at": c.get("updated_at"),
            }
        )
    conn.close()
    return pd.DataFrame(rows)


def _default_prev_calendar_month_utc() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    m = now.month - 1
    y = now.year
    if m == 0:
        m = 12
        y -= 1
    return y, m


def _fetch_dynamic_accumulation_zones_for_sheet(symbol: str) -> pd.DataFrame:
    """Зоны накопления (1m, календарный месяц UTC) — см. dynamic_accumulation_zones."""
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

    year: int | None = None
    month: int | None = None
    if DYNAMIC_ZONES_YEAR and DYNAMIC_ZONES_MONTH:
        year = int(DYNAMIC_ZONES_YEAR)
        month = int(DYNAMIC_ZONES_MONTH)

    if year is None or month is None:
        year, month = _default_prev_calendar_month_utc()

    dz_kwargs: dict = {"rescan": False, "top_n_per_band": 0}
    gap_used: float = float(DEFAULT_CLUSTER_MERGE_MAX_GAP_PCT)
    if DYNAMIC_ZONES_CLUSTER_MERGE_GAP_PCT is not None and str(DYNAMIC_ZONES_CLUSTER_MERGE_GAP_PCT).strip() != "":
        gap_used = float(DYNAMIC_ZONES_CLUSTER_MERGE_GAP_PCT)
    elif DYNAMIC_ZONES_WEIGHTED_MERGE_PCT is not None and str(DYNAMIC_ZONES_WEIGHTED_MERGE_PCT).strip() != "":
        gap_used = float(DYNAMIC_ZONES_WEIGHTED_MERGE_PCT)
    dz_kwargs["cluster_merge_max_gap_pct"] = gap_used if gap_used > 0 else None
    dz_kwargs["cluster_merge_max_time_gap_hours"] = _parse_cluster_merge_time_gap_hours(
        DYNAMIC_ZONES_CLUSTER_MERGE_TIME_GAP_HOURS
    )

    if DYNAMIC_ZONES_BIN_STEP_USDT:
        dz_kwargs["zone_bin_step_usdt"] = float(DYNAMIC_ZONES_BIN_STEP_USDT)
    poc_thr_used = (
        float(DYNAMIC_ZONES_POC_THRESHOLD_PCT)
        if DYNAMIC_ZONES_POC_THRESHOLD_PCT
        else DEFAULT_POC_MERGE_THRESHOLD_PCT
    )
    if DYNAMIC_ZONES_POC_THRESHOLD_PCT:
        dz_kwargs["poc_merge_threshold_pct"] = poc_thr_used
    cluster_used = (
        float(DYNAMIC_ZONES_CLUSTER_PCT)
        if DYNAMIC_ZONES_CLUSTER_PCT
        else DEFAULT_CLUSTER_THRESHOLD_PCT
    )
    if DYNAMIC_ZONES_CLUSTER_PCT:
        dz_kwargs["cluster_threshold_pct"] = cluster_used
        dz_kwargs["rescan"] = True

    top_n_used: int | None = 0
    if DYNAMIC_ZONES_TOP_N_PER_BAND is not None and DYNAMIC_ZONES_TOP_N_PER_BAND != "":
        v = max(0, int(DYNAMIC_ZONES_TOP_N_PER_BAND))
        dz_kwargs["top_n_per_band"] = v
        top_n_used = v
        if v > 0:
            dz_kwargs["rescan"] = True
    band_w_used: float | None = None
    if DYNAMIC_ZONES_PRICE_BAND_USDT:
        band_w_used = float(DYNAMIC_ZONES_PRICE_BAND_USDT)
        dz_kwargs["price_band_usdt"] = band_w_used

    out, bin_step = run_pipeline(
        df,
        year=year,
        month=month,
        **dz_kwargs,
    )
    exported_at = datetime.now(timezone.utc).isoformat()

    if out.empty:
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": f"{year}-{month:02d}",
                    "bin_step_usdt": round(float(bin_step), 6) if bin_step else "",
                    "poc_merge_threshold_pct": poc_thr_used,
                    "cluster_threshold_pct": cluster_used,
                    "top_n_per_band": top_n_used if top_n_used is not None else 0,
                    "price_band_usdt": band_w_used
                    if band_w_used is not None
                    else (DEFAULT_PRICE_BAND_TICK_MULTIPLIER * float(bin_step) if bin_step else ""),
                    "rescan": dz_kwargs.get("rescan", False),
                    "cluster_merge_max_gap_pct": dz_kwargs.get("cluster_merge_max_gap_pct") or 0.0,
                    "cluster_merge_max_time_gap_hours": dz_kwargs.get("cluster_merge_max_time_gap_hours"),
                    "note": "Нет зон или нет 1m данных за выбранный месяц",
                    "exported_at_utc": exported_at,
                }
            ]
        )

    out = out.copy()
    out.insert(0, "symbol", symbol)
    out.insert(1, "month_utc", f"{year}-{month:02d}")
    out["t_start_utc"] = out["t_start_unix"].map(lambda x: _ts_to_iso_utc(int(x)))
    out["t_end_utc"] = out["t_end_unix"].map(lambda x: _ts_to_iso_utc(int(x)))
    out["bin_step_usdt"] = round(float(bin_step), 6)
    out["poc_merge_threshold_pct"] = poc_thr_used
    out["cluster_threshold_pct"] = cluster_used
    if band_w_used is not None:
        band_meta = band_w_used
    else:
        band_meta = DEFAULT_PRICE_BAND_TICK_MULTIPLIER * float(bin_step)
    out["top_n_per_band"] = top_n_used if top_n_used is not None else 0
    out["price_band_usdt"] = round(float(band_meta), 4)
    out["rescan"] = bool(dz_kwargs.get("rescan", False))
    out["cluster_merge_max_gap_pct"] = float(dz_kwargs.get("cluster_merge_max_gap_pct") or 0.0)
    _tg = dz_kwargs.get("cluster_merge_max_time_gap_hours")
    out["cluster_merge_max_time_gap_hours"] = "" if _tg is None else float(_tg)
    out["exported_at_utc"] = exported_at
    cols = [
        "symbol",
        "month_utc",
        "exported_at_utc",
        "bin_step_usdt",
        "poc_merge_threshold_pct",
        "cluster_threshold_pct",
        "rescan",
        "top_n_per_band",
        "price_band_usdt",
        "cluster_merge_max_gap_pct",
        "cluster_merge_max_time_gap_hours",
        "Цена уровня",
        "Суммарный объем",
        "Время жизни (ч)",
        "Сила (Tier)",
        "t_start_utc",
        "t_end_utc",
        "t_start_unix",
        "t_end_unix",
    ]
    return out[[c for c in cols if c in out.columns]]


def _fetch_volume_peak_levels_for_sheet(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Пики профиля объёма (1m) + таблица аудита (raw/dedup/final, тиры) для листа анализа."""
    symbols = [s.strip() for s in str(symbol).split(",") if s.strip()]
    if len(symbols) > 1:
        dfs: List[pd.DataFrame] = []
        auds: List[pd.DataFrame] = []
        for s in symbols:
            d_l, d_a = _fetch_volume_peak_levels_for_sheet(s)
            dfs.append(d_l)
            auds.append(d_a)
        return (
            pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(),
            pd.concat(auds, ignore_index=True) if auds else pd.DataFrame(),
        )
    sym = symbols[0] if symbols else str(symbol).strip()
    if not sym:
        return pd.DataFrame([{"note": "Не указан символ"}]), pd.DataFrame()
    return _volume_peak_levels_one(sym)


def _volume_peak_levels_one(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Один символ: уровни для листа + одна строка аудита."""
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

    year: int | None = None
    month: int | None = None
    if DYNAMIC_ZONES_YEAR and DYNAMIC_ZONES_MONTH:
        year = int(DYNAMIC_ZONES_YEAR)
        month = int(DYNAMIC_ZONES_MONTH)
    if year is None or month is None:
        year, month = _default_prev_calendar_month_utc()

    exported_at = datetime.now(timezone.utc).isoformat()
    month_label = f"{year}-{month:02d}"
    base_meta = {"symbol": symbol, "month_utc": month_label, "exported_at_utc": exported_at}

    work = slice_calendar_month_utc(df, year, month)
    if work.empty:
        row = {**base_meta, "note": "Нет 1m данных за выбранный месяц"}
        audit = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": month_label,
                    "exported_at_utc": exported_at,
                    "strict_raw_n": 0,
                    "strict_dedup_n": 0,
                    "final_levels_n": 0,
                    "tier1_beton_n": 0,
                    "tier2_n": 0,
                    "tier3_n": 0,
                    "two_pass_mode": "",
                    "run_soft_pass": "",
                    "legacy_weak_merge": "",
                    "note": row["note"],
                }
            ]
        )
        return pd.DataFrame([row]), audit

    params = get_adaptive_params(work)
    height_mult: float | None = float(PRO_LEVELS_HEIGHT_MULT) if PRO_LEVELS_HEIGHT_MULT else None
    distance_pct = (
        float(PRO_LEVELS_DISTANCE_PCT) if PRO_LEVELS_DISTANCE_PCT else float(params["distance_pct"])
    )
    valley_threshold = (
        float(PRO_LEVELS_VALLEY_THRESHOLD)
        if PRO_LEVELS_VALLEY_THRESHOLD
        else float(params["valley_threshold"])
    )
    tick_size = float(params["tick_size"])
    avg_hourly_volatility = float(params["avg_hourly_volatility"])
    volume_cv = float(params["volume_cv"])
    top_n = int(params.get("top_n", 10))
    min_duration_hours = float(params.get("min_duration_hours", 6.0))
    max_levels: int | None = params.get("max_levels")
    if PRO_LEVELS_MIN_DURATION_HOURS is not None and str(PRO_LEVELS_MIN_DURATION_HOURS).strip() != "":
        min_duration_hours = float(PRO_LEVELS_MIN_DURATION_HOURS)
    if PRO_LEVELS_MAX_LEVELS is not None and str(PRO_LEVELS_MAX_LEVELS).strip() != "":
        max_levels = int(PRO_LEVELS_MAX_LEVELS)
    include_all_tiers = True
    if PRO_LEVELS_INCLUDE_ALL_TIERS is not None and str(PRO_LEVELS_INCLUDE_ALL_TIERS).strip() != "":
        include_all_tiers = str(PRO_LEVELS_INCLUDE_ALL_TIERS).strip().lower() not in ("0", "false", "no", "off")
    final_merge_pct: float | None = params.get("dynamic_merge_pct")
    if PRO_LEVELS_FINAL_MERGE_PCT is not None and str(PRO_LEVELS_FINAL_MERGE_PCT).strip() != "":
        final_merge_pct = float(PRO_LEVELS_FINAL_MERGE_PCT)
    valley_merge_threshold = float(params.get("valley_merge_threshold", 0.5))
    if PRO_LEVELS_VALLEY_MERGE_THRESHOLD is not None and str(PRO_LEVELS_VALLEY_MERGE_THRESHOLD).strip() != "":
        valley_merge_threshold = float(PRO_LEVELS_VALLEY_MERGE_THRESHOLD)
    enable_valley_merge = str(PRO_LEVELS_ENABLE_VALLEY_MERGE).strip().lower() == "true"
    dedup_round_pct = float(params.get("dedup_round_pct", 0.001))
    if PRO_LEVELS_DEDUP_ROUND_PCT is not None and str(PRO_LEVELS_DEDUP_ROUND_PCT).strip() != "":
        dedup_round_pct = float(PRO_LEVELS_DEDUP_ROUND_PCT)
    final_merge_valley_threshold: float | None = params.get("final_merge_valley_threshold")
    if (
        PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD is not None
        and str(PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD).strip() != ""
    ):
        final_merge_valley_threshold = float(PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD)

    legacy_weak_merge = str(PRO_LEVELS_LEGACY_WEAK_MERGE).strip().lower() in ("1", "true", "yes", "on")
    run_soft_pass = str(PRO_LEVELS_RUN_SOFT_PASS).strip().lower() not in ("0", "false", "no", "off")
    strict_height_percentile_weak: float | None = None
    if PRO_LEVELS_STRICT_HEIGHT_WEAK is not None and str(PRO_LEVELS_STRICT_HEIGHT_WEAK).strip() != "":
        strict_height_percentile_weak = float(PRO_LEVELS_STRICT_HEIGHT_WEAK)
    strict_height_mult: float | None = None
    if PRO_LEVELS_STRICT_HEIGHT_MULT is not None and str(PRO_LEVELS_STRICT_HEIGHT_MULT).strip() != "":
        strict_height_mult = float(PRO_LEVELS_STRICT_HEIGHT_MULT)
    soft_height_percentile_strong = 0.6
    soft_height_percentile_weak = 0.55
    if PRO_LEVELS_SOFT_HEIGHT_STRONG is not None and str(PRO_LEVELS_SOFT_HEIGHT_STRONG).strip() != "":
        soft_height_percentile_strong = float(PRO_LEVELS_SOFT_HEIGHT_STRONG)
    if PRO_LEVELS_SOFT_HEIGHT_WEAK is not None and str(PRO_LEVELS_SOFT_HEIGHT_WEAK).strip() != "":
        soft_height_percentile_weak = float(PRO_LEVELS_SOFT_HEIGHT_WEAK)
    soft_height_mult: float | None = None
    if PRO_LEVELS_SOFT_HEIGHT_MULT is not None and str(PRO_LEVELS_SOFT_HEIGHT_MULT).strip() != "":
        soft_height_mult = float(PRO_LEVELS_SOFT_HEIGHT_MULT)
    soft_final_merge_pct: float | None = None
    if PRO_LEVELS_SOFT_FINAL_MERGE_PCT is not None and str(PRO_LEVELS_SOFT_FINAL_MERGE_PCT).strip() != "":
        soft_final_merge_pct = float(PRO_LEVELS_SOFT_FINAL_MERGE_PCT)
    exclude_reserved_pct: float | None = None
    if PRO_LEVELS_EXCLUDE_RESERVED_PCT is not None and str(PRO_LEVELS_EXCLUDE_RESERVED_PCT).strip() != "":
        exclude_reserved_pct = float(PRO_LEVELS_EXCLUDE_RESERVED_PCT)
    soft_min_duration_hours = 4.0
    if PRO_LEVELS_WEAK_MIN_DURATION is not None and str(PRO_LEVELS_WEAK_MIN_DURATION).strip() != "":
        soft_min_duration_hours = float(PRO_LEVELS_WEAK_MIN_DURATION)

    height_percentile_strong = float(params.get("height_percentile_strong", 0.85))
    height_percentile_weak = float(params.get("height_percentile_weak", 0.65))
    two_pass_mode = not legacy_weak_merge

    real_min_tick = float(params.get("real_min_tick", 0.0))
    price_band_usdt = float(params.get("price_band_usdt", 0.0))
    base_meta = {
        **base_meta,
        "height_mult": height_mult,
        "distance_pct": distance_pct,
        "valley_threshold": valley_threshold,
        "tick_size": tick_size,
        "avg_hourly_volatility": avg_hourly_volatility,
        "volume_cv": volume_cv,
        "top_n": top_n,
        "min_duration_hours": min_duration_hours,
        "max_levels": "" if max_levels is None else max_levels,
        "include_all_tiers": include_all_tiers,
        "final_merge_pct": "" if final_merge_pct is None else final_merge_pct,
        "valley_merge_threshold": valley_merge_threshold,
        "enable_valley_merge": enable_valley_merge,
        "dedup_round_pct": dedup_round_pct,
        "final_merge_valley_threshold": (
            "" if final_merge_valley_threshold is None else final_merge_valley_threshold
        ),
        "legacy_weak_merge": legacy_weak_merge,
        "run_soft_pass": run_soft_pass,
        "two_pass_mode": two_pass_mode,
        "height_percentile_strong": height_percentile_strong,
        "height_percentile_weak": height_percentile_weak,
        "strict_height_percentile_weak": (
            "" if strict_height_percentile_weak is None else strict_height_percentile_weak
        ),
        "strict_height_mult": "" if strict_height_mult is None else strict_height_mult,
        "soft_height_percentile_strong": soft_height_percentile_strong,
        "soft_height_percentile_weak": soft_height_percentile_weak,
        "soft_height_mult": "" if soft_height_mult is None else soft_height_mult,
        "soft_min_duration_hours": soft_min_duration_hours,
        "soft_final_merge_pct": "" if soft_final_merge_pct is None else soft_final_merge_pct,
        "exclude_reserved_pct": "" if exclude_reserved_pct is None else exclude_reserved_pct,
        "real_min_tick": real_min_tick,
        "price_band_usdt": price_band_usdt,
    }

    try:
        common_kw = dict(
            height_mult=height_mult,
            distance_pct=distance_pct,
            valley_threshold=valley_threshold,
            tick_size=tick_size,
            top_n=top_n,
            min_duration_hours=min_duration_hours,
            max_levels=max_levels,
            include_all_tiers=include_all_tiers,
            final_merge_pct=final_merge_pct,
            valley_merge_threshold=valley_merge_threshold,
            enable_valley_merge=enable_valley_merge,
            allow_stage_b_overlap=True,
            dedup_round_pct=dedup_round_pct,
            final_merge_valley_threshold=final_merge_valley_threshold,
            legacy_weak_merge=legacy_weak_merge,
            two_pass_mode=two_pass_mode,
            run_soft_pass=run_soft_pass,
            height_percentile_strong=height_percentile_strong,
            height_percentile_weak=height_percentile_weak,
            strict_height_percentile_weak=strict_height_percentile_weak,
            strict_height_mult=strict_height_mult,
            exclude_reserved_pct=exclude_reserved_pct,
            soft_height_percentile_strong=soft_height_percentile_strong,
            soft_height_percentile_weak=soft_height_percentile_weak,
            soft_height_mult=soft_height_mult,
            soft_min_duration_hours=soft_min_duration_hours,
            soft_final_merge_pct=soft_final_merge_pct,
        )
        raw_levels = find_pro_levels(work, **common_kw, return_raw=True)
        dedup_levels = find_pro_levels(work, **common_kw, return_dedup=True)
        final_levels = find_pro_levels(work, **common_kw)
        print(
            f"[{symbol} {month_label}] levels raw={len(raw_levels)} dedup={len(dedup_levels)} final={len(final_levels)} "
            f"merge_pct={final_merge_pct} valley_merge={enable_valley_merge} vm_thr={valley_merge_threshold}"
        )
    except RuntimeError as e:
        row = {**base_meta, "note": str(e)}
        audit = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": month_label,
                    "exported_at_utc": exported_at,
                    "strict_raw_n": 0,
                    "strict_dedup_n": 0,
                    "final_levels_n": 0,
                    "tier1_beton_n": 0,
                    "tier2_n": 0,
                    "tier3_n": 0,
                    "two_pass_mode": two_pass_mode,
                    "run_soft_pass": run_soft_pass,
                    "legacy_weak_merge": legacy_weak_merge,
                    "note": str(e),
                }
            ]
        )
        return pd.DataFrame([row]), audit

    if final_levels.empty:
        row = {**base_meta, "note": "Нет пиков по заданным height_mult / distance_pct"}
        audit = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": month_label,
                    "exported_at_utc": exported_at,
                    "strict_raw_n": len(raw_levels),
                    "strict_dedup_n": len(dedup_levels),
                    "final_levels_n": 0,
                    "tier1_beton_n": 0,
                    "tier2_n": 0,
                    "tier3_n": 0,
                    "two_pass_mode": two_pass_mode,
                    "run_soft_pass": run_soft_pass,
                    "legacy_weak_merge": legacy_weak_merge,
                    "note": row["note"],
                }
            ]
        )
        return pd.DataFrame([row]), audit

    out = final_levels.copy()
    out.insert(0, "symbol", symbol)
    out.insert(1, "month_utc", month_label)
    out["exported_at_utc"] = exported_at
    out["height_mult"] = height_mult
    out["distance_pct"] = distance_pct
    out["valley_threshold"] = valley_threshold
    out["tick_size"] = tick_size
    out["avg_hourly_volatility"] = avg_hourly_volatility
    out["volume_cv"] = volume_cv
    out["top_n"] = top_n
    out["min_duration_hours"] = min_duration_hours
    out["max_levels"] = "" if max_levels is None else max_levels
    out["include_all_tiers"] = include_all_tiers
    out["final_merge_pct"] = "" if final_merge_pct is None else final_merge_pct
    out["valley_merge_threshold"] = valley_merge_threshold
    out["enable_valley_merge"] = enable_valley_merge
    out["dedup_round_pct"] = dedup_round_pct
    out["final_merge_valley_threshold"] = (
        "" if final_merge_valley_threshold is None else final_merge_valley_threshold
    )
    out["legacy_weak_merge"] = legacy_weak_merge
    out["run_soft_pass"] = run_soft_pass
    out["two_pass_mode"] = two_pass_mode
    out["height_percentile_strong"] = height_percentile_strong
    out["height_percentile_weak"] = height_percentile_weak
    out["strict_height_percentile_weak"] = (
        "" if strict_height_percentile_weak is None else strict_height_percentile_weak
    )
    out["strict_height_mult"] = "" if strict_height_mult is None else strict_height_mult
    out["soft_height_percentile_strong"] = soft_height_percentile_strong
    out["soft_height_percentile_weak"] = soft_height_percentile_weak
    out["soft_height_mult"] = "" if soft_height_mult is None else soft_height_mult
    out["soft_min_duration_hours"] = soft_min_duration_hours
    out["soft_final_merge_pct"] = "" if soft_final_merge_pct is None else soft_final_merge_pct
    out["exclude_reserved_pct"] = "" if exclude_reserved_pct is None else exclude_reserved_pct
    out["real_min_tick"] = real_min_tick
    out["price_band_usdt"] = price_band_usdt
    out = out.rename(
        columns={
            "Price": "Цена уровня",
            "Volume": "Суммарный объем",
            "Duration_Hrs": "Время жизни (ч)",
            "Tier": "Сила (Tier)",
        }
    )
    cols = [
        "symbol",
        "month_utc",
        "exported_at_utc",
        "height_mult",
        "distance_pct",
        "valley_threshold",
        "tick_size",
        "avg_hourly_volatility",
        "volume_cv",
        "top_n",
        "min_duration_hours",
        "max_levels",
        "include_all_tiers",
        "final_merge_pct",
        "valley_merge_threshold",
        "enable_valley_merge",
        "dedup_round_pct",
        "final_merge_valley_threshold",
        "legacy_weak_merge",
        "run_soft_pass",
        "two_pass_mode",
        "height_percentile_strong",
        "height_percentile_weak",
        "strict_height_percentile_weak",
        "strict_height_mult",
        "soft_height_percentile_strong",
        "soft_height_percentile_weak",
        "soft_height_mult",
        "soft_min_duration_hours",
        "soft_final_merge_pct",
        "exclude_reserved_pct",
        "real_min_tick",
        "price_band_usdt",
        "Цена уровня",
        "Суммарный объем",
        "Время жизни (ч)",
        "Сила (Tier)",
        "start_utc",
        "end_utc",
    ]
    out = out[[c for c in cols if c in out.columns]]
    if not final_levels.empty and "Tier" in final_levels.columns:
        t1 = int((final_levels["Tier"] == "Tier 1 (Бетон)").sum())
        ser = final_levels["Tier"].astype(str)
        t2 = int(ser.str.contains("Tier 2", na=False).sum())
        t3 = int(ser.str.contains("Tier 3", na=False).sum())
    else:
        t1 = t2 = t3 = 0
    audit = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "month_utc": month_label,
                "exported_at_utc": exported_at,
                "strict_raw_n": len(raw_levels),
                "strict_dedup_n": len(dedup_levels),
                "final_levels_n": len(final_levels),
                "tier1_beton_n": t1,
                "tier2_n": t2,
                "tier3_n": t3,
                "two_pass_mode": two_pass_mode,
                "run_soft_pass": run_soft_pass,
                "legacy_weak_merge": legacy_weak_merge,
                "dedup_round_pct": dedup_round_pct,
                "distance_pct": distance_pct,
                "final_merge_pct": "" if final_merge_pct is None else final_merge_pct,
                "note": "",
            }
        ]
    )
    return out, audit


def _build_volume_peaks_analysis_sheet(audit_df: pd.DataFrame) -> pd.DataFrame:
    """Человекочитаемая сводка для отдельного листа Google Sheets."""
    if audit_df.empty:
        return pd.DataFrame(
            [{"Раздел": "Анализ уровней (volume profile)", "Содержание": "Нет строк аудита."}]
        )

    def _interpret(row: pd.Series) -> str:
        note = str(row.get("note") or "").strip()
        if note:
            return note
        fr, fd, fn = int(row.get("strict_raw_n") or 0), int(row.get("strict_dedup_n") or 0), int(
            row.get("final_levels_n") or 0
        )
        t1, t2, t3 = int(row.get("tier1_beton_n") or 0), int(row.get("tier2_n") or 0), int(
            row.get("tier3_n") or 0
        )
        parts = [
            f"Жёсткий проход: raw={fr} → dedup={fd} (стадии по одному символу/месяцу).",
            f"Финальный вывод: {fn} уровн.; Tier1 (бетон)={t1}, Tier2={t2}, Tier3={t3}.",
        ]
        if row.get("two_pass_mode") is True and t1 == 0 and fn > 0:
            parts.append(
                "Двухпроходный режим без Tier1 в жёстком dedup: в финале в основном мягкие уровни."
            )
        elif row.get("two_pass_mode") is True and t1 > 0:
            parts.append("Есть зафиксированные сильные зоны (Tier1 из жёсткого dedup).")
        if row.get("legacy_weak_merge") is True:
            parts.append("Использован legacy single-pass (legacy_weak_merge).")
        drop = fr - fd
        if drop > 0:
            parts.append(f"Дедупликация по цене убрала {drop} дублей относительно raw.")
        if fn < fd:
            parts.append("Финальный merge дополнительно сократил число близких уровней.")
        return " ".join(parts)

    out = audit_df.copy()
    out["Интерпретация"] = out.apply(_interpret, axis=1)
    return out.rename(
        columns={
            "symbol": "Символ",
            "month_utc": "Месяц_UTC",
            "exported_at_utc": "Экспорт_UTC",
            "strict_raw_n": "Жёсткий_raw",
            "strict_dedup_n": "Жёсткий_dedup",
            "final_levels_n": "Финал_уровней",
            "tier1_beton_n": "Tier1_бетон",
            "tier2_n": "Tier2",
            "tier3_n": "Tier3",
            "two_pass_mode": "Два_прохода",
            "run_soft_pass": "Мягкий_проход",
            "legacy_weak_merge": "Legacy_merge",
            "dedup_round_pct": "dedup_round_pct",
            "distance_pct": "distance_pct",
            "final_merge_pct": "final_merge_pct",
            "note": "Примечание",
        }
    )


def main() -> None:
    init_db()
    exporter = SheetsExporter(
        credentials_path=CREDENTIALS_PATH,
        spreadsheet_title=SHEET_TITLE,
        spreadsheet_url=SHEET_URL,
        spreadsheet_id=SHEET_ID,
    )
    exported_at = datetime.now(timezone.utc).isoformat()
    audit_entries: List[Dict[str, Any]] = []

    crypto_symbols = sorted(set(TRADING_SYMBOLS + ANALYTIC_SYMBOLS.get("crypto", [])))
    df_binance = _fetch_ohlcv_sample(
        symbols=crypto_symbols,
        timeframes=["1h", "4h", "1d", "1w", "1M"],
        limit=100,
    )
    exporter.export_dataframe_to_sheet(df_binance, SHEET_TITLE, "binance_ohlcv_sample")
    audit_entries.append(
        {
            "source": SOURCE_BINANCE,
            "worksheet": "binance_ohlcv_sample",
            "rows": len(df_binance),
            "last_exported_at_utc": exported_at,
        }
    )

    df_macro = _fetch_ohlcv_sample(
        symbols=ANALYTIC_SYMBOLS.get("macro", []),
        timeframes=["4h", "1d", "1w", "1M"],
        limit=100,
        fill_weekends=FILL_MISSING_WEEKENDS,
    )
    exporter.export_dataframe_to_sheet(df_macro, SHEET_TITLE, "macro_sample")
    audit_entries.append(
        {
            "source": "macro",
            "worksheet": "macro_sample",
            "rows": len(df_macro),
            "last_exported_at_utc": exported_at,
        }
    )

    df_indices = _fetch_ohlcv_sample(
        symbols=ANALYTIC_SYMBOLS.get("indices", []),
        timeframes=["1m", "4h", "1d", "1w", "1M"],
        limit=100,
    )
    exporter.export_dataframe_to_sheet(df_indices, SHEET_TITLE, "indices_sample")
    audit_entries.append(
        {
            "source": "indices",
            "worksheet": "indices_sample",
            "rows": len(df_indices),
            "last_exported_at_utc": exported_at,
        }
    )

    df_indices_agg = _fetch_indices_agg_sample(limit=200)
    exporter.export_dataframe_to_sheet(df_indices_agg, SHEET_TITLE, "indices_agg_sample")
    audit_entries.append(
        {
            "source": "coingecko_agg",
            "worksheet": "indices_agg_sample",
            "rows": len(df_indices_agg),
            "last_exported_at_utc": exported_at,
        }
    )

    df_cg_full = _fetch_all_coingecko_agg()
    exporter.export_dataframe_to_sheet(df_cg_full, SHEET_TITLE, "coingecko_agg_all")
    audit_entries.append(
        {
            "source": "coingecko_agg",
            "worksheet": "coingecko_agg_all",
            "rows": len(df_cg_full),
            "last_exported_at_utc": exported_at,
        }
    )

    df_val_candles = _fetch_ohlcv_sample(
        symbols=["BTC/USDT", "ETH/USDT", "SP500"],
        timeframes=["1h", "4h", "1d"],
        limit=10,
        fill_weekends=FILL_MISSING_WEEKENDS,
    )
    exporter.export_dataframe_to_sheet(df_val_candles, SHEET_TITLE, "validation_candles_1h_4h_1d")
    audit_entries.append(
        {
            "source": "validation",
            "worksheet": "validation_candles_1h_4h_1d",
            "rows": len(df_val_candles),
            "last_exported_at_utc": exported_at,
        }
    )

    df_dyn_zones = _fetch_dynamic_accumulation_zones_for_sheet(DYNAMIC_ZONES_SYMBOL)
    exporter.export_dataframe_to_sheet(df_dyn_zones, SHEET_TITLE, "dynamic_accumulation_zones")
    audit_entries.append(
        {
            "source": "dynamic_accumulation_zones",
            "worksheet": "dynamic_accumulation_zones",
            "rows": len(df_dyn_zones),
            "last_exported_at_utc": exported_at,
        }
    )

    peak_symbols = resolve_volume_peak_export_symbols()
    df_peak_levels, df_peak_audit = _fetch_volume_peak_levels_for_sheet(peak_symbols)
    exporter.export_dataframe_to_sheet(
        df_peak_levels, SHEET_TITLE, VOLUME_PEAK_LEVELS_WORKSHEET
    )
    df_peak_analysis = _build_volume_peaks_analysis_sheet(df_peak_audit)
    exporter.export_dataframe_to_sheet(
        df_peak_analysis, SHEET_TITLE, VOLUME_PEAK_ANALYSIS_WORKSHEET
    )
    audit_entries.append(
        {
            "source": "volume_profile_peaks",
            "worksheet": VOLUME_PEAK_LEVELS_WORKSHEET,
            "rows": len(df_peak_levels),
            "last_exported_at_utc": exported_at,
        }
    )
    audit_entries.append(
        {
            "source": "volume_profile_peaks_analysis",
            "worksheet": VOLUME_PEAK_ANALYSIS_WORKSHEET,
            "rows": len(df_peak_analysis),
            "last_exported_at_utc": exported_at,
        }
    )

    df_coinglass = _fetch_coinglass_sample(limit=50)
    exporter.export_dataframe_to_sheet(df_coinglass, SHEET_TITLE, "coinglass_sample")
    audit_entries.append(
        {
            "source": "binance_futures",
            "worksheet": "coinglass_sample",
            "rows": len(df_coinglass),
            "last_exported_at_utc": exported_at,
        }
    )

    df_log = _build_audit_log(audit_entries)
    exporter.export_dataframe_to_sheet(df_log, SHEET_TITLE, "audit_log")


if __name__ == "__main__":
    main()
