from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import pandas as pd

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

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
from trading_bot.data.repositories import get_level_events_since, get_ohlcv_filled
from trading_bot.config.settings import (
    LEVEL_EVENTS_CONFIRM_ATR_PCT,
    LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR,
    VOLUME_PEAK_LEVELS_WORKSHEET,
)
from trading_bot.data.volume_profile_peaks_db import LEVEL_TYPE_VOLUME_PROFILE_PEAKS
from trading_bot.tools.sheets_exporter import SheetsExporter

SHEET_TITLE = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_URL = os.getenv("MARKET_AUDIT_SHEET_URL")
SHEET_ID = os.getenv("MARKET_AUDIT_SHEET_ID")
DYNAMIC_ZONES_SYMBOL = os.getenv("DYNAMIC_ZONES_SYMBOL", "BTC/USDT")
# Список пар для листов volume profile (через запятую); если пусто — три пары по умолчанию.
VOLUME_PEAK_SYMBOLS = os.getenv("VOLUME_PEAK_SYMBOLS")
DEFAULT_VOLUME_PEAK_SYMBOLS = ",".join(TRADING_SYMBOLS)


def resolve_volume_peak_export_symbols() -> str:
    # По требованию проекта: всегда берём символы из `trading_bot/config/symbols.py`.
    # env-переопределение `VOLUME_PEAK_SYMBOLS` не используется.
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
# Имя листа vp_local: trading_bot.config.settings.VOLUME_PEAK_LEVELS_WORKSHEET (env VOLUME_PEAK_LEVELS_WORKSHEET / DBSCAN_ZONES_WORKSHEET).
VOLUME_PEAK_ANALYSIS_WORKSHEET = os.getenv(
    "VOLUME_PEAK_ANALYSIS_WORKSHEET", "volume_profile_peaks_analysis"
)
LEVEL_EVENTS_WORKSHEET = os.getenv("LEVEL_EVENTS_WORKSHEET", "level_events")
LEVEL_STRENGTH_WORKSHEET = os.getenv("LEVEL_STRENGTH_WORKSHEET", "level_strength_report")
LEVEL_STOP_PROFILE_WORKSHEET = os.getenv("LEVEL_STOP_PROFILE_WORKSHEET", "level_stop_profile")

# События с итоговым исходом для strength/stop: подтверждённый отбой/пробой или ложный пробой (возврат без confirm).
_LEVEL_STRENGTH_QUALIFYING_STATUSES = frozenset(
    {
        "confirmed_rebound_up",
        "confirmed_rebound_down",
        "confirmed_breakout_up",
        "confirmed_breakout_down",
        "false_break",
    }
)

# Важно: пики volume_profile_peaks в Google Sheets теперь выгружаются
# строго из SQLite `price_levels` (посчитанные и сохраненные скриптом rebuild).
# Поэтому параметры `PRO_LEVELS_*` больше не читаются из env здесь.


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
    now_iso = datetime.now(timezone.utc).isoformat()

    # Активные уровни текущего слоя: при save через merge UPDATE не меняет created_at,
    # поэтому отбор только по MAX(created_at) отрезает часть строк одного прогона в Sheets.
    # Берём layer с самой свежей COALESCE(updated_at, created_at), затем все активные строки этого layer.
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT layer FROM price_levels
        WHERE symbol = ? AND level_type = ? AND is_active = 1
        ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        (symbol, LEVEL_TYPE_VOLUME_PROFILE_PEAKS),
    )
    row_layer = cur.fetchone()
    ref_layer = (
        str(row_layer["layer"])
        if row_layer is not None and row_layer["layer"] is not None and str(row_layer["layer"]).strip() != ""
        else None
    )
    if ref_layer is None:
        conn.close()
        out = pd.DataFrame([{"symbol": symbol, "month_utc": "", "exported_at_utc": now_iso, "note": "Нет активных уровней"}])
        audit = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": "",
                    "exported_at_utc": now_iso,
                    "strict_raw_n": 0,
                    "strict_dedup_n": 0,
                    "final_levels_n": 0,
                    "tier1_beton_n": 0,
                    "tier2_n": 0,
                    "tier3_n": 0,
                    "two_pass_mode": "",
                    "run_soft_pass": "",
                    "legacy_weak_merge": "",
                    "dedup_round_pct": "",
                    "distance_pct": "",
                    "final_merge_pct": "",
                    "note": "Нет активных levels в price_levels",
                }
            ]
        )
        return out, audit

    cur.execute(
        """
        SELECT
            layer,
            price,
            volume_peak,
            duration_hours,
            tier,
            t_start_unix,
            t_end_unix,
            created_at
        FROM price_levels
        WHERE symbol = ? AND level_type = ? AND is_active = 1 AND layer = ?
        ORDER BY volume_peak DESC, price ASC
        """,
        (symbol, LEVEL_TYPE_VOLUME_PROFILE_PEAKS, ref_layer),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    if not rows:
        out = pd.DataFrame([{"symbol": symbol, "month_utc": "", "exported_at_utc": now_iso, "note": "Нет строк"}])
        audit = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "month_utc": "",
                    "exported_at_utc": now_iso,
                    "strict_raw_n": 0,
                    "strict_dedup_n": 0,
                    "final_levels_n": 0,
                    "tier1_beton_n": 0,
                    "tier2_n": 0,
                    "tier3_n": 0,
                    "two_pass_mode": "",
                    "run_soft_pass": "",
                    "legacy_weak_merge": "",
                    "dedup_round_pct": "",
                    "distance_pct": "",
                    "final_merge_pct": "",
                    "note": "Нет строк активных levels в price_levels",
                }
            ]
        )
        return out, audit

    # month_utc: если задан env, используем его; иначе — из t_end_unix первого уровня.
    month_label: str
    if DYNAMIC_ZONES_YEAR and DYNAMIC_ZONES_MONTH:
        year = int(DYNAMIC_ZONES_YEAR)
        month = int(DYNAMIC_ZONES_MONTH)
        month_label = f"{year}-{month:02d}"
    else:
        end_unix = rows[0].get("t_end_unix")
        if end_unix is not None:
            dt = datetime.fromtimestamp(int(end_unix), tz=timezone.utc)
            month_label = f"{dt.year}-{dt.month:02d}"
        else:
            prev_y, prev_m = _default_prev_calendar_month_utc()
            month_label = f"{prev_y}-{prev_m:02d}"

    def _ts_to_iso(v: object) -> str:
        if v is None:
            return ""
        try:
            return _ts_to_iso_utc(int(v))
        except Exception:
            return ""

    out = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "month_utc": month_label,
                "exported_at_utc": now_iso,
                "level_type": LEVEL_TYPE_VOLUME_PROFILE_PEAKS,
                "layer": r.get("layer"),
                "Цена уровня": float(r.get("price")) if r.get("price") is not None else None,
                "Суммарный объем": float(r.get("volume_peak")) if r.get("volume_peak") is not None else None,
                "Время жизни (ч)": float(r.get("duration_hours")) if r.get("duration_hours") is not None else None,
                "Сила (Tier)": str(r.get("tier")) if r.get("tier") is not None else "",
                "start_utc": _ts_to_iso(r.get("t_start_unix")),
                "end_utc": _ts_to_iso(r.get("t_end_unix")),
            }
            for r in rows
        ]
    )

    t1 = int((out["Сила (Tier)"] == "Tier 1 (Бетон)").sum())
    t2 = int(out["Сила (Tier)"].astype(str).str.contains("Tier 2", na=False).sum())
    t3 = int(out["Сила (Tier)"].astype(str).str.contains("Tier 3", na=False).sum())

    audit = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "month_utc": month_label,
                "exported_at_utc": now_iso,
                "strict_raw_n": 0,
                "strict_dedup_n": 0,
                "final_levels_n": len(out),
                "tier1_beton_n": t1,
                "tier2_n": t2,
                "tier3_n": t3,
                "two_pass_mode": "",
                "run_soft_pass": "",
                "legacy_weak_merge": "",
                "dedup_round_pct": "",
                "distance_pct": "",
                "final_merge_pct": "",
                "note": "Считано из price_levels (is_active=1)",
            }
        ]
    )

    # Сверяемся с предыдущими ожиданиями: оставляем только колонки, которые реально нужны/существуют.
    cols = [
        "symbol",
        "month_utc",
        "exported_at_utc",
        "level_type",
        "layer",
        "Цена уровня",
        "Суммарный объем",
        "Время жизни (ч)",
        "Сила (Tier)",
        "start_utc",
        "end_utc",
    ]
    out = out[[c for c in cols if c in out.columns]]
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


def _fetch_level_events_for_sheet(lookback_days: int = 30) -> pd.DataFrame:
    start_ts = int(datetime.now(timezone.utc).timestamp()) - int(lookback_days * 86400)
    rows = get_level_events_since(start_ts)
    if not rows:
        return pd.DataFrame([{"note": "Нет событий уровней за выбранный период."}])
    out = pd.DataFrame(rows)
    if "event_status" in out.columns:
        out["event_status"] = out["event_status"].fillna("legacy_unknown")
        out = out[out["event_status"] != "legacy_unknown"].copy()
    if out.empty:
        return pd.DataFrame([{"note": "Нет событий нового алгоритма за выбранный период."}])
    # Backward compatibility: older DB schema does not store *_pct metrics.
    if "atr_pct" not in out.columns and {"atr_daily", "level_price"}.issubset(out.columns):
        out["atr_pct"] = (out["atr_daily"].astype(float) / out["level_price"].astype(float)) * 100.0
    if "penetration_pct" not in out.columns and {"penetration_atr", "atr_daily", "level_price"}.issubset(out.columns):
        out["penetration_pct"] = (
            out["penetration_atr"].astype(float) * out["atr_daily"].astype(float) / out["level_price"].astype(float) * 100.0
        )
    if "rebound_pure_pct" not in out.columns and {"rebound_pure_atr", "atr_daily", "level_price"}.issubset(out.columns):
        out["rebound_pure_pct"] = (
            out["rebound_pure_atr"].astype(float) * out["atr_daily"].astype(float) / out["level_price"].astype(float) * 100.0
        )
    if "rebound_after_return_pct" not in out.columns and {"rebound_after_return_atr", "atr_daily", "level_price"}.issubset(out.columns):
        out["rebound_after_return_pct"] = (
            out["rebound_after_return_atr"].astype(float) * out["atr_daily"].astype(float) / out["level_price"].astype(float) * 100.0
        )
    # ATR-relative percentages (canonical for analytics).
    if "dist_start_atr" in out.columns:
        out["dist_start_atr_pct"] = pd.to_numeric(out["dist_start_atr"], errors="coerce") * 100.0
    if "penetration_atr" in out.columns:
        out["penetration_atr_pct"] = pd.to_numeric(out["penetration_atr"], errors="coerce") * 100.0
    if "rebound_pure_atr" in out.columns:
        out["rebound_pure_atr_pct"] = pd.to_numeric(out["rebound_pure_atr"], errors="coerce") * 100.0
    if "rebound_after_return_atr" in out.columns:
        out["rebound_after_return_atr_pct"] = pd.to_numeric(out["rebound_after_return_atr"], errors="coerce") * 100.0
    for c in ("touch_time", "return_time", "window_start", "window_end", "created_at"):
        if c in out.columns:
            name = f"{c}_utc"
            out[name] = out[c].apply(
                lambda v: _ts_to_iso_utc(int(v)) if pd.notna(v) else ""
            )
    cols = [
        "symbol",
        "stable_level_id",
        "event_status",
        "pre_side",
        "level_price",
        "touch_time_utc",
        "confirm_time_sec",
        "touch_count_before_confirm",
        "dist_start_atr_pct",
        "penetration_atr_pct",
        "rebound_pure_atr_pct",
        "rebound_after_return_atr_pct",
        "cluster_size",
    ]
    return out[[c for c in cols if c in out.columns]]


def _prepare_level_events_for_strength_export(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()
    if "event_status" in df.columns:
        df["event_status"] = df["event_status"].fillna("legacy_unknown")
        df = df[df["event_status"] != "legacy_unknown"].copy()
    if df.empty:
        return pd.DataFrame()
    if "atr_pct" not in df.columns and {"atr_daily", "level_price"}.issubset(df.columns):
        df["atr_pct"] = (df["atr_daily"].astype(float) / df["level_price"].astype(float)) * 100.0
    if "penetration_atr_pct" not in df.columns and "penetration_atr" in df.columns:
        df["penetration_atr_pct"] = pd.to_numeric(df["penetration_atr"], errors="coerce") * 100.0
    if "rebound_pure_atr_pct" not in df.columns and "rebound_pure_atr" in df.columns:
        df["rebound_pure_atr_pct"] = pd.to_numeric(df["rebound_pure_atr"], errors="coerce") * 100.0
    if "rebound_after_return_atr_pct" not in df.columns and "rebound_after_return_atr" in df.columns:
        df["rebound_after_return_atr_pct"] = pd.to_numeric(
            df["rebound_after_return_atr"], errors="coerce"
        ) * 100.0
    return df


def _aggregate_level_strength_by_level(df: pd.DataFrame) -> pd.DataFrame:
    """Одна строка на уровень: контекст для merge с событиями (composite, broken, touches_n, …)."""
    if df.empty:
        return pd.DataFrame()
    group_cols = ["symbol", "stable_level_id", "tier", "layer", "level_price"]
    rows_out: List[Dict[str, Any]] = []
    recent_n = 5
    for keys, g in df.groupby(group_cols, dropna=False):
        g = g.sort_values("touch_time", ascending=False)
        touches_n = int(len(g))
        if "event_status" in g.columns:
            g_eval = g[
                g["event_status"].isin(
                    [
                        "confirmed_rebound_up",
                        "confirmed_rebound_down",
                        "confirmed_breakout_up",
                        "confirmed_breakout_down",
                        "open",
                        "stale_open",
                    ]
                )
            ].copy()
            returned = g_eval[g_eval["event_status"].isin(["confirmed_rebound_up", "confirmed_rebound_down"])].copy()
            breakouts = g_eval[g_eval["event_status"].isin(["confirmed_breakout_up", "confirmed_breakout_down"])].copy()
            confirmed_n = len(returned) + len(breakouts)
            return_rate = float(len(returned) / confirmed_n) if confirmed_n > 0 else 0.0
            break_rate = float(len(breakouts) / confirmed_n) if confirmed_n > 0 else 0.0
        else:
            returned = g[g["return_time"].notna()].copy()
            return_rate = float(len(returned) / touches_n) if touches_n > 0 else 0.0
            break_rate = max(0.0, 1.0 - return_rate)

        pen_all = pd.to_numeric(g["penetration_atr_pct"], errors="coerce").dropna()
        pen_ret = pd.to_numeric(returned["penetration_atr_pct"], errors="coerce").dropna()
        reb_after = pd.to_numeric(returned["rebound_after_return_atr_pct"], errors="coerce").dropna()
        reb_pure = pd.to_numeric(g["rebound_pure_atr_pct"], errors="coerce").dropna()
        atr_pct_s = pd.to_numeric(g["atr_pct"], errors="coerce").dropna()
        cluster = pd.to_numeric(g["cluster_size"], errors="coerce").dropna()

        rec = g.head(recent_n)
        recent_no_return_n = int(rec["return_time"].isna().sum())
        recent_pen_p80 = (
            float(pd.to_numeric(rec["penetration_atr_pct"], errors="coerce").dropna().quantile(0.8))
            if len(rec)
            else 0.0
        )
        recent_break_streak = 0
        for _, rr in rec.iterrows():
            if pd.notna(rr.get("return_time")):
                break
            recent_break_streak += 1

        rows_out.append(
            {
                "symbol": keys[0],
                "stable_level_id": keys[1],
                "tier": keys[2],
                "layer": keys[3],
                "level_price": keys[4],
                "touches_n": touches_n,
                "return_rate": return_rate,
                "break_rate": break_rate,
                "p50_penetration_atr_pct": float(pen_all.median()) if not pen_all.empty else None,
                "p80_penetration_atr_pct": float(pen_all.quantile(0.8)) if not pen_all.empty else None,
                "p95_penetration_atr_pct": float(pen_all.quantile(0.95)) if not pen_all.empty else None,
                "p80_penetration_atr_pct_returned": float(pen_ret.quantile(0.8)) if not pen_ret.empty else None,
                "median_rebound_after_atr_pct": float(reb_after.median()) if not reb_after.empty else None,
                "median_rebound_pure_atr_pct": float(reb_pure.median()) if not reb_pure.empty else None,
                "lvl_median_atr_pct": float(atr_pct_s.median()) if not atr_pct_s.empty else None,
                "median_cluster_size": float(cluster.median()) if not cluster.empty else None,
                "recent_no_return_n": recent_no_return_n,
                "recent_break_streak": recent_break_streak,
                "recent_penetration_p80_pct": recent_pen_p80,
            }
        )

    out = pd.DataFrame(rows_out)
    out["pen_score"] = (1.0 - (out["p80_penetration_atr_pct"].fillna(0.0) / 120.0)).clip(0.0, 1.0)
    out["reb_score"] = (out["median_rebound_after_atr_pct"].fillna(0.0) / 120.0).clip(0.0, 1.0)
    out["sample_score"] = out["touches_n"].fillna(0.0).apply(
        lambda x: min(1.0, (0.0 if x <= 0 else (math.log1p(float(x)) / math.log1p(30.0))))
    )
    out["noise_score"] = (1.0 - ((out["median_cluster_size"].fillna(1.0) - 1.0) / 4.0)).clip(0.0, 1.0)
    out["composite_score"] = (
        100.0
        * (
            0.30 * out["return_rate"].fillna(0.0)
            + 0.20 * out["pen_score"]
            + 0.20 * out["reb_score"]
            + 0.15 * out["sample_score"]
            + 0.10 * out["noise_score"]
            + 0.05 * (1.0 - out["break_rate"])
        )
    ).round(2)
    out["strength_bucket"] = out["composite_score"].apply(
        lambda s: "strong" if s >= 80 else ("medium" if s >= 60 else "weak")
    )
    out["broken_flag"] = (
        ((out["break_rate"].fillna(0.0) >= 0.60) & (out["return_rate"].fillna(0.0) <= 0.40))
        | (out["recent_break_streak"].fillna(0) >= 3)
        | (
            (out["recent_no_return_n"].fillna(0) >= 3)
            & (out["recent_penetration_p80_pct"].fillna(0.0) >= 60.0)
        )
    ).astype(int)
    base_stop_raw = pd.to_numeric(out["p80_penetration_atr_pct_returned"], errors="coerce") + 10.0
    atr_floor = pd.Series(float(LEVEL_EVENTS_CONFIRM_ATR_PCT) * 100.0, index=out.index)
    out["lvl_recommended_stop_pct_base"] = pd.concat([base_stop_raw, atr_floor], axis=1).max(axis=1).round(4)
    out["lvl_recommended_stop_pct_conservative"] = (
        pd.concat(
            [
                out["lvl_recommended_stop_pct_base"] + 15.0,
                atr_floor * 1.25,
            ],
            axis=1,
        )
        .max(axis=1)
        .round(4)
    )
    out["stop_floor_pct"] = atr_floor.round(4)
    out.loc[
        out["p80_penetration_atr_pct_returned"].isna() & atr_floor.isna(),
        ["lvl_recommended_stop_pct_base", "lvl_recommended_stop_pct_conservative"],
    ] = pd.NA
    return out


def _fetch_level_strength_report_for_sheet(lookback_days: int = 90, compact: bool = True) -> pd.DataFrame:
    start_ts = int(datetime.now(timezone.utc).timestamp()) - int(lookback_days * 86400)
    rows = get_level_events_since(start_ts)
    if not rows:
        return pd.DataFrame([{"note": "Нет событий для расчета силы уровней."}])

    df = _prepare_level_events_for_strength_export(rows)
    if df.empty:
        return pd.DataFrame([{"note": "Нет событий нового алгоритма для расчета силы."}])

    agg = _aggregate_level_strength_by_level(df)
    merge_keys = ["symbol", "stable_level_id", "tier", "layer", "level_price"]
    min_reb = float(LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR)
    qual = df[df["event_status"].isin(_LEVEL_STRENGTH_QUALIFYING_STATUSES)].copy()
    qual = qual[pd.to_numeric(qual["rebound_pure_atr"], errors="coerce") >= min_reb]
    if qual.empty:
        return pd.DataFrame(
            [
                {
                    "note": (
                        f"Нет событий с rebound_pure ≥ {min_reb * 100:.0f}% ATR "
                        "и итоговым статусом (confirm / false_break) за период."
                    )
                }
            ]
        )

    out = qual.merge(agg, on=merge_keys, how="left")
    out = out.sort_values(["symbol", "touch_time"], ascending=[True, False])
    for c in ("touch_time", "return_time", "window_start", "window_end"):
        if c in out.columns:
            out[f"{c}_utc"] = out[c].apply(
                lambda v: _ts_to_iso_utc(int(v)) if pd.notna(v) else "",
            )
    out["exported_at_utc"] = datetime.now(timezone.utc).isoformat()
    if not compact:
        return out
    cols = [
        "symbol",
        "stable_level_id",
        "event_id",
        "event_status",
        "pre_side",
        "level_price",
        "touch_time_utc",
        "penetration_atr_pct",
        "rebound_pure_atr_pct",
        "rebound_after_return_atr_pct",
        "confirm_time_sec",
        "touch_count_before_confirm",
        "cluster_size",
        "touches_n",
        "return_rate",
        "break_rate",
        "broken_flag",
        "composite_score",
        "strength_bucket",
        "p80_penetration_atr_pct",
        "median_rebound_after_atr_pct",
        "lvl_recommended_stop_pct_base",
        "lvl_recommended_stop_pct_conservative",
        "exported_at_utc",
    ]
    return out[[c for c in cols if c in out.columns]]


def _fetch_level_stop_profile_for_sheet(lookback_days: int = 90) -> pd.DataFrame:
    start_ts = int(datetime.now(timezone.utc).timestamp()) - int(lookback_days * 86400)
    rows = get_level_events_since(start_ts)
    if not rows:
        return pd.DataFrame([{"note": "Нет данных для stop profile."}])

    df = _prepare_level_events_for_strength_export(rows)
    if df.empty:
        return pd.DataFrame([{"note": "Нет данных для stop profile."}])

    agg = _aggregate_level_strength_by_level(df)
    merge_keys = ["symbol", "stable_level_id", "tier", "layer", "level_price"]
    min_reb = float(LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR)
    qual = df[df["event_status"].isin(_LEVEL_STRENGTH_QUALIFYING_STATUSES)].copy()
    qual = qual[pd.to_numeric(qual["rebound_pure_atr"], errors="coerce") >= min_reb]
    if qual.empty:
        return pd.DataFrame(
            [
                {
                    "note": (
                        f"Нет событий с rebound_pure ≥ {min_reb * 100:.0f}% ATR "
                        "для stop profile за период."
                    )
                }
            ]
        )

    out = qual.merge(agg, on=merge_keys, how="left")
    if "stable_level_id" not in out.columns:
        return pd.DataFrame([{"note": "Нет данных для stop profile."}])

    conn = get_connection()
    cur = conn.cursor()
    atr_by_symbol: Dict[str, float] = {}
    for row in cur.execute(
        "SELECT symbol, atr FROM instruments WHERE exchange='bybit_futures' AND atr IS NOT NULL"
    ).fetchall():
        s = str(row["symbol"] or "")
        if s.endswith("USDT") and "/" not in s:
            s = s[:-4] + "/USDT"
        atr_by_symbol[s] = float(row["atr"])
    conn.close()

    out["atr_daily"] = out["symbol"].map(atr_by_symbol)
    out = out[out["atr_daily"].notna()].copy()
    if out.empty:
        return pd.DataFrame([{"note": "Нет ATR по символам для stop profile."}])

    pen_pct = pd.to_numeric(out["penetration_atr_pct"], errors="coerce")
    atr_floor_pct = float(LEVEL_EVENTS_CONFIRM_ATR_PCT) * 100.0
    out["recommended_stop_pct_base"] = pd.concat(
        [pen_pct + 10.0, pd.Series(atr_floor_pct, index=out.index)], axis=1
    ).max(axis=1).round(4)
    out["recommended_stop_pct_conservative"] = (
        pd.concat(
            [
                out["recommended_stop_pct_base"] + 15.0,
                pd.Series(atr_floor_pct * 1.25, index=out.index),
            ],
            axis=1,
        )
        .max(axis=1)
        .round(4)
    )

    out["stop_price_long_base"] = out["level_price"].astype(float) - (
        pd.to_numeric(out["recommended_stop_pct_base"], errors="coerce") / 100.0
    ) * out["atr_daily"].astype(float)
    out["stop_price_short_base"] = out["level_price"].astype(float) + (
        pd.to_numeric(out["recommended_stop_pct_base"], errors="coerce") / 100.0
    ) * out["atr_daily"].astype(float)
    out["stop_price_long_conservative"] = out["level_price"].astype(float) - (
        pd.to_numeric(out["recommended_stop_pct_conservative"], errors="coerce") / 100.0
    ) * out["atr_daily"].astype(float)
    out["stop_price_short_conservative"] = out["level_price"].astype(float) + (
        pd.to_numeric(out["recommended_stop_pct_conservative"], errors="coerce") / 100.0
    ) * out["atr_daily"].astype(float)
    out["break_boundary_price"] = out["level_price"].astype(float) - (
        float(LEVEL_EVENTS_CONFIRM_ATR_PCT) * out["atr_daily"].astype(float)
    )
    out["stop_formula_atr_pct"] = (
        (
            (out["level_price"].astype(float) - out["break_boundary_price"].astype(float)).abs()
            * 100.0
            / out["atr_daily"].astype(float)
        ).round(4)
    )
    out["valid_from_utc"] = datetime.now(timezone.utc).isoformat()
    out["valid_to_utc"] = ""
    out["trade_allowed"] = (
        (out["broken_flag"].fillna(0).astype(int) == 0)
        & pd.to_numeric(out["recommended_stop_pct_base"], errors="coerce").notna()
        & (out["touches_n"].fillna(0).astype(int) >= 5)
    ).astype(int)
    out["deny_reason"] = out.apply(
        lambda r: (
            "broken"
            if int(r.get("broken_flag", 0) or 0) == 1
            else (
                "no_stop"
                if pd.isna(r.get("recommended_stop_pct_base"))
                else ("low_sample" if int(r.get("touches_n", 0) or 0) < 5 else "")
            )
        ),
        axis=1,
    )
    out["quality_gate"] = out["trade_allowed"].map({1: "ALLOW", 0: "DENY"})
    out.loc[out["broken_flag"] == 1, [
        "recommended_stop_pct_base",
        "recommended_stop_pct_conservative",
        "stop_price_long_base",
        "stop_price_short_base",
        "stop_price_long_conservative",
        "stop_price_short_conservative",
    ]] = pd.NA
    cols = [
        "symbol",
        "stable_level_id",
        "event_id",
        "event_status",
        "level_price",
        "broken_flag",
        "trade_allowed",
        "deny_reason",
        "stop_formula_atr_pct",
        "recommended_stop_pct_base",
        "recommended_stop_pct_conservative",
        "break_boundary_price",
        "valid_from_utc",
    ]
    return out[[c for c in cols if c in out.columns]]


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

    crypto_symbols = sorted(
        set(
            TRADING_SYMBOLS
            + ANALYTIC_SYMBOLS.get("crypto_context", [])
            + ANALYTIC_SYMBOLS.get("crypto", []),
        )
    )
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

    df_level_events = _fetch_level_events_for_sheet(lookback_days=30)
    exporter.export_dataframe_to_sheet(df_level_events, SHEET_TITLE, LEVEL_EVENTS_WORKSHEET)
    audit_entries.append(
        {
            "source": "level_events",
            "worksheet": LEVEL_EVENTS_WORKSHEET,
            "rows": len(df_level_events),
            "last_exported_at_utc": exported_at,
        }
    )

    df_level_strength = _fetch_level_strength_report_for_sheet(lookback_days=90)
    exporter.export_dataframe_to_sheet(df_level_strength, SHEET_TITLE, LEVEL_STRENGTH_WORKSHEET)
    audit_entries.append(
        {
            "source": "level_strength",
            "worksheet": LEVEL_STRENGTH_WORKSHEET,
            "rows": len(df_level_strength),
            "last_exported_at_utc": exported_at,
        }
    )

    df_level_stops = _fetch_level_stop_profile_for_sheet(lookback_days=90)
    exporter.export_dataframe_to_sheet(df_level_stops, SHEET_TITLE, LEVEL_STOP_PROFILE_WORKSHEET)
    audit_entries.append(
        {
            "source": "level_stop_profile",
            "worksheet": LEVEL_STOP_PROFILE_WORKSHEET,
            "rows": len(df_level_stops),
            "last_exported_at_utc": exported_at,
        }
    )

    df_log = _build_audit_log(audit_entries)
    exporter.export_dataframe_to_sheet(df_log, SHEET_TITLE, "audit_log")


if __name__ == "__main__":
    main()
