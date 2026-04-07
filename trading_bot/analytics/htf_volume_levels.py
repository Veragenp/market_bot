"""
Высокоуровневый (HTF) объёмный профиль: тот же `find_pro_levels`, другой ТФ и окно в днях.

Не трогает `level_type=vp_local` (1m-месячный пайплайн). Пишет `vp_global` с merge по `stable_level_id`.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import pandas as pd

from config import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.analytics.volume_profile_peaks import find_pro_levels, get_adaptive_params
from trading_bot.config.settings import (
    DEFAULT_SOURCE_BINANCE,
    DEFAULT_SOURCE_TRADINGVIEW,
    DEFAULT_SOURCE_YFINANCE,
    HTF_LEVELS_DISABLE_SHEETS,
    HTF_LEVELS_DURATION_TIER1_H,
    HTF_LEVELS_DURATION_TIER2_H,
    HTF_LEVELS_LOOKBACK_DAYS,
    HTF_LEVELS_MIN_BARS,
    HTF_LEVELS_MIN_DURATION_HOURS,
    HTF_LEVELS_RUN_SOFT_PASS,
    HTF_LEVELS_SHEET_ID,
    HTF_LEVELS_SHEET_TITLE,
    HTF_LEVELS_SHEET_URL,
    HTF_LEVELS_SHEET_WORKSHEET,
    HTF_LEVELS_TIMEFRAME,
    HTF_LEVELS_TOP_N,
)
from trading_bot.data.repositories import get_ohlcv
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_TYPE_VOLUME_PROFILE_HTF,
    save_volume_profile_peaks_levels_to_db,
)
from trading_bot.tools.sheets_exporter import SheetsExporter

logger = logging.getLogger(__name__)


def _normalize_timeframe(tf: str) -> str:
    t = (tf or "1d").strip()
    if t == "1W":
        return "1w"
    return t


def iter_config_symbols_with_source() -> Iterator[tuple[str, str]]:
    seen: set[str] = set()
    for s in TRADING_SYMBOLS:
        seen.add(s)
        yield s, DEFAULT_SOURCE_BINANCE
    # Контекстные спот-пары (намеренный дубль в конфиге допустим; здесь не обрабатываем дважды)
    for s in ANALYTIC_SYMBOLS.get("crypto_context", []):
        if s in seen:
            continue
        seen.add(s)
        yield s, DEFAULT_SOURCE_BINANCE
    for s in ANALYTIC_SYMBOLS.get("crypto", []):
        if s in seen:
            continue
        seen.add(s)
        yield s, DEFAULT_SOURCE_BINANCE
    for s in ANALYTIC_SYMBOLS.get("macro", []):
        yield s, DEFAULT_SOURCE_YFINANCE
    for s in ANALYTIC_SYMBOLS.get("indices", []):
        yield s, DEFAULT_SOURCE_TRADINGVIEW


def _fetch_htf_ohlcv_dataframe(
    symbol: str,
    source: str,
    timeframe: str,
    lookback_days: int,
) -> pd.DataFrame | None:
    now_ts = int(time.time())
    start_ts = now_ts - int(lookback_days) * 86400
    rows = get_ohlcv(
        symbol=symbol,
        timeframe=timeframe,
        start=start_ts,
        end=now_ts,
        source=source,
    )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    if df.empty:
        return None
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            logger.warning("HTF skip %s: missing column %s", symbol, col)
            return None
    return df


def _build_layer_tag(timeframe: str, lookback_days: int, start_ts: int, end_ts: int) -> str:
    return f"htf_{timeframe}_{lookback_days}d_{start_ts}_{end_ts}"


def _find_pro_kwargs_htf(df: pd.DataFrame, symbol: str) -> dict[str, Any]:
    p = get_adaptive_params(df, symbol=symbol)
    hm = p.get("height_mult")
    dyn_merge = p.get("dynamic_merge_pct")
    fmv = p.get("final_merge_valley_threshold")
    return {
        "smoothing_window": int(p.get("smoothing_window", 5)),
        "height_percentile": float(p.get("height_percentile", 0.8)),
        "height_percentile_strong": float(p.get("height_percentile_strong", 0.85)),
        "height_percentile_weak": float(p.get("height_percentile_weak", 0.65)),
        "distance_pct": float(p["distance_pct"]),
        "valley_threshold": float(p["valley_threshold"]),
        "merge_distance_pct": float(p.get("merge_distance_pct", 0.001)),
        "duration_thresholds": (float(HTF_LEVELS_DURATION_TIER1_H), float(HTF_LEVELS_DURATION_TIER2_H)),
        "tick_size": float(p["tick_size"]),
        "height_mult": float(hm) if hm is not None else None,
        "top_n": int(HTF_LEVELS_TOP_N),
        "min_duration_hours": float(HTF_LEVELS_MIN_DURATION_HOURS),
        "max_levels": p.get("max_levels"),
        "final_merge_pct": float(dyn_merge) if dyn_merge is not None else None,
        "valley_merge_threshold": float(p.get("valley_merge_threshold", 0.5)),
        "enable_valley_merge": bool(p.get("enable_valley_merge", True)),
        "allow_stage_b_overlap": True,
        "dedup_round_pct": float(p.get("dedup_round_pct", 0.001)),
        "final_merge_valley_threshold": float(fmv) if fmv is not None else None,
        "two_pass_mode": True,
        "run_soft_pass": bool(HTF_LEVELS_RUN_SOFT_PASS),
        "soft_height_percentile_strong": 0.6,
        "soft_height_percentile_weak": 0.55,
        "soft_height_mult": None,
        "soft_min_duration_hours": max(1.0, float(HTF_LEVELS_MIN_DURATION_HOURS)),
        "soft_final_merge_pct": None,
        "include_weak": True,
        "symbol": symbol,
    }


def check_htf_ohlcv_coverage(
    *,
    lookback_days: int | None = None,
    timeframe: str | None = None,
) -> list[dict[str, Any]]:
    """
    Диагностика: сколько свечей попадёт в HTF-окно с фильтром source (как в расчёте)
    и сколько без фильтра (если >0 при нуле с source — несовпадение источника в БД).
    """
    lb = int(lookback_days if lookback_days is not None else HTF_LEVELS_LOOKBACK_DAYS)
    tf = _normalize_timeframe(timeframe if timeframe is not None else HTF_LEVELS_TIMEFRAME)
    now_ts = int(time.time())
    start_ts = now_ts - lb * 86400
    out: list[dict[str, Any]] = []
    for symbol, source in iter_config_symbols_with_source():
        with_src = get_ohlcv(
            symbol=symbol,
            timeframe=tf,
            start=start_ts,
            end=now_ts,
            source=source,
        )
        any_src = get_ohlcv(
            symbol=symbol,
            timeframe=tf,
            start=start_ts,
            end=now_ts,
            source=None,
        )
        out.append(
            {
                "symbol": symbol,
                "expected_source": source,
                "timeframe": tf,
                "lookback_days": lb,
                "rows_with_source": len(with_src),
                "rows_any_source": len(any_src),
                "ok": len(with_src) >= int(HTF_LEVELS_MIN_BARS),
            }
        )
    return out


def run_htf_volume_levels_batch(
    *,
    lookback_days: int | None = None,
    timeframe: str | None = None,
    level_type: str | None = None,
) -> tuple[int, pd.DataFrame]:
    """
    Для всех символов из `TRADING_SYMBOLS` + доп. спот из `ANALYTIC_SYMBOLS['crypto_context']`
    (без повтора пар из торгового списка) + `macro`, `indices`:
    загрузка OHLCV, `find_pro_levels`, сохранение в `price_levels`.

    `level_type` по умолчанию `vp_global`; для экспериментов передайте отдельную строку,
    чтобы не снимать активность с основного HTF-слоя.

    Returns:
        (число символов с ненулевым числом сохранённых уровней, объединённый DataFrame для Sheets)
    """
    lb = int(lookback_days if lookback_days is not None else HTF_LEVELS_LOOKBACK_DAYS)
    tf = _normalize_timeframe(timeframe if timeframe is not None else HTF_LEVELS_TIMEFRAME)
    now_ts = int(time.time())
    start_ts = now_ts - lb * 86400

    lt = level_type if level_type is not None else LEVEL_TYPE_VOLUME_PROFILE_HTF

    sheet_rows: list[dict[str, Any]] = []
    saved_with_levels = 0

    for symbol, source in iter_config_symbols_with_source():
        df = _fetch_htf_ohlcv_dataframe(symbol, source, tf, lb)
        if df is None:
            logger.warning("HTF skip %s: no OHLCV (tf=%s source=%s, last %s d)", symbol, tf, source, lb)
            continue
        if len(df) < int(HTF_LEVELS_MIN_BARS):
            logger.warning(
                "HTF skip %s: only %s bars (need >= %s)",
                symbol,
                len(df),
                HTF_LEVELS_MIN_BARS,
            )
            continue
        try:
            kwargs = _find_pro_kwargs_htf(df, symbol)
            out = find_pro_levels(df, **kwargs)
        except Exception:
            logger.exception("HTF find_pro_levels failed for %s", symbol)
            continue

        layer = _build_layer_tag(tf, lb, start_ts, now_ts)
        save_df = out if out is not None and not out.empty else pd.DataFrame()
        save_volume_profile_peaks_levels_to_db(
            symbol,
            save_df,
            layer=layer,
            level_type=lt,
            timeframe=tf,
        )
        if out is not None and not out.empty:
            saved_with_levels += 1
            logger.info("HTF saved %s: levels=%s layer=%s", symbol, len(out), layer)
        else:
            logger.info("HTF %s: no levels, active merged set archived (level_type=%s)", symbol, lt)

        if out is not None and not out.empty:
            exported_at = datetime.now(timezone.utc).isoformat()
            for _, row in out.iterrows():
                sheet_rows.append(
                    {
                        "level_type": lt,
                        "symbol": symbol,
                        "ohlcv_source": source,
                        "timeframe": tf,
                        "lookback_days": lb,
                        "layer": layer,
                        "Price": row.get("Price"),
                        "Volume": row.get("Volume"),
                        "Duration_Hrs": row.get("Duration_Hrs"),
                        "Tier": row.get("Tier"),
                        "start_utc": row.get("start_utc"),
                        "end_utc": row.get("end_utc"),
                        "exported_at_utc": exported_at,
                    }
                )

    combined = pd.DataFrame(sheet_rows)
    return saved_with_levels, combined


def export_htf_levels_to_sheets(df: pd.DataFrame, *, worksheet_name: str | None = None) -> None:
    if HTF_LEVELS_DISABLE_SHEETS:
        logger.info("HTF Sheets export skipped (HTF_LEVELS_DISABLE_SHEETS)")
        return
    cred = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    audit_title = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
    audit_url = os.getenv("MARKET_AUDIT_SHEET_URL")
    audit_id = os.getenv("MARKET_AUDIT_SHEET_ID")

    if HTF_LEVELS_SHEET_ID:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_id=HTF_LEVELS_SHEET_ID)
    elif HTF_LEVELS_SHEET_URL:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_url=HTF_LEVELS_SHEET_URL)
    elif HTF_LEVELS_SHEET_TITLE:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_title=HTF_LEVELS_SHEET_TITLE)
    elif audit_id:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_id=audit_id)
    elif audit_url:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_url=audit_url)
    elif audit_title:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_title=audit_title)
    else:
        logger.warning("HTF Sheets: set HTF_LEVELS_SHEET_* or MARKET_AUDIT_SHEET_* ; skip export")
        return
    ws = worksheet_name if worksheet_name else HTF_LEVELS_SHEET_WORKSHEET
    if df.empty:
        logger.info("HTF Sheets: empty dataframe, writing header only")
    exporter.export_dataframe_to_sheet(df, "HTF export", ws)
    logger.info("HTF Sheets: wrote worksheet %s (%s rows)", ws, len(df))


__all__ = [
    "check_htf_ohlcv_coverage",
    "export_htf_levels_to_sheets",
    "iter_config_symbols_with_source",
    "run_htf_volume_levels_batch",
]
