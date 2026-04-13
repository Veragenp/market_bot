"""
Пакетный расчёт человеческих уровней (D1/W1) → SQLite + опционально Google Sheets.

Символы и источники OHLCV — как у HTF (`iter_config_symbols_with_source`).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from trading_bot.analytics.htf_volume_levels import iter_config_symbols_with_source
from trading_bot.config.settings import (
    DEFAULT_SOURCE_BINANCE,
    HUMAN_LEVELS_CLUSTER_ATR_MULT,
    HUMAN_LEVELS_D1_LOOKBACK_DAYS,
    HUMAN_LEVELS_DISABLE_SHEETS,
    HUMAN_LEVELS_MIN_BARS_D1,
    HUMAN_LEVELS_MIN_BARS_W1,
    HUMAN_LEVELS_SHEET_ID,
    HUMAN_LEVELS_SHEET_TITLE,
    HUMAN_LEVELS_SHEET_URL,
    HUMAN_LEVELS_SHEET_WORKSHEET,
    HUMAN_LEVELS_W1_LOOKBACK_DAYS,
)
from trading_bot.data.db import get_connection
from trading_bot.data.human_levels_db import run_human_levels_and_save
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur, get_ohlcv
from trading_bot.tools.sheets_exporter import SheetsExporter

logger = logging.getLogger(__name__)


def export_human_levels_to_sheets(df: pd.DataFrame, *, worksheet_name: str | None = None) -> None:
    if HUMAN_LEVELS_DISABLE_SHEETS:
        logger.info("Human levels Sheets export skipped (HUMAN_LEVELS_DISABLE_SHEETS)")
        return
    cred = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    audit_title = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
    audit_url = os.getenv("MARKET_AUDIT_SHEET_URL")
    audit_id = os.getenv("MARKET_AUDIT_SHEET_ID")

    if HUMAN_LEVELS_SHEET_ID:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_id=HUMAN_LEVELS_SHEET_ID)
    elif HUMAN_LEVELS_SHEET_URL:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_url=HUMAN_LEVELS_SHEET_URL)
    elif HUMAN_LEVELS_SHEET_TITLE:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_title=HUMAN_LEVELS_SHEET_TITLE)
    elif audit_id:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_id=audit_id)
    elif audit_url:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_url=audit_url)
    elif audit_title:
        exporter = SheetsExporter(credentials_path=cred, spreadsheet_title=audit_title)
    else:
        logger.warning(
            "Human levels Sheets: set HUMAN_LEVELS_SHEET_* or MARKET_AUDIT_SHEET_* ; skip export"
        )
        return
    ws = worksheet_name if worksheet_name else HUMAN_LEVELS_SHEET_WORKSHEET
    if df.empty:
        logger.info("Human levels Sheets: empty dataframe, writing header only")
    exporter.export_dataframe_to_sheet(df, "Human levels export", ws)
    logger.info("Human levels Sheets: wrote worksheet %s (%s rows)", ws, len(df))


def run_human_levels_batch_job(*, export_sheets: bool = True) -> tuple[int, pd.DataFrame]:
    """
    Для каждого символа из конфига: загрузка 1d/1w за окна из settings,
    сохранение в price_levels, строки для Sheets.

    Returns:
        (число символов с хотя бы одной зоной, DataFrame для Sheets)
    """
    now_ts = int(time.time())
    start_d1 = now_ts - int(HUMAN_LEVELS_D1_LOOKBACK_DAYS) * 86400
    start_w1 = now_ts - int(HUMAN_LEVELS_W1_LOOKBACK_DAYS) * 86400
    layer = (
        f"human_auto_d{HUMAN_LEVELS_D1_LOOKBACK_DAYS}d_w{HUMAN_LEVELS_W1_LOOKBACK_DAYS}d_{now_ts}"
    )

    sheet_rows: list[dict[str, Any]] = []
    symbols_with_zones = 0

    conn_atr = get_connection()
    cur_atr = conn_atr.cursor()
    try:
        for symbol, source in iter_config_symbols_with_source():
            rows_d1 = get_ohlcv(symbol, "1d", start=start_d1, end=now_ts, source=source)
            rows_w1 = get_ohlcv(symbol, "1w", start=start_w1, end=now_ts, source=source)
            if len(rows_d1) < HUMAN_LEVELS_MIN_BARS_D1:
                logger.warning(
                    "Human levels skip %s: 1d bars=%s (need >= %s) source=%s",
                    symbol,
                    len(rows_d1),
                    HUMAN_LEVELS_MIN_BARS_D1,
                    source,
                )
                continue
            if len(rows_w1) < HUMAN_LEVELS_MIN_BARS_W1:
                logger.warning(
                    "Human levels skip %s: 1w bars=%s (need >= %s) source=%s",
                    symbol,
                    len(rows_w1),
                    HUMAN_LEVELS_MIN_BARS_W1,
                    source,
                )
                continue

            df_d1 = pd.DataFrame(rows_d1)
            df_w1 = pd.DataFrame(rows_w1)

            try:
                atr_db = get_instruments_atr_bybit_futures_cur(cur_atr, symbol)
                result = run_human_levels_and_save(
                    symbol,
                    df_d1,
                    df_w1,
                    layer=layer,
                    now_ts=now_ts,
                    atr_d1=atr_db,
                    cluster_atr_mult=HUMAN_LEVELS_CLUSTER_ATR_MULT,
                )
            except Exception:
                logger.exception("Human levels failed for %s", symbol)
                continue

            n_z = len(result.zones_d1) + len(result.zones_w1)
            if n_z > 0:
                symbols_with_zones += 1

            exported_at = datetime.now(timezone.utc).isoformat()
            t0_d = int(df_d1["timestamp"].min()) if not df_d1.empty else None
            t1_d = int(df_d1["timestamp"].max()) if not df_d1.empty else None
            t0_w = int(df_w1["timestamp"].min()) if not df_w1.empty else None
            t1_w = int(df_w1["timestamp"].max()) if not df_w1.empty else None

            for z in list(result.zones_d1) + list(result.zones_w1):
                sheet_rows.append(
                    {
                        "symbol": symbol,
                        "ohlcv_source": source,
                        "zone_timeframe": z.timeframe,
                        "zone_low": z.zone_low,
                        "zone_high": z.zone_high,
                        "price_mid": (z.zone_low + z.zone_high) / 2.0,
                        "strength": z.strength,
                        "fractal_count": z.fractal_count,
                        "atr_d1_last": result.atr_d1_last,
                        "atr_w1_equiv": result.atr_w1_equiv,
                        "lookback_d1_days": HUMAN_LEVELS_D1_LOOKBACK_DAYS,
                        "lookback_w1_days": HUMAN_LEVELS_W1_LOOKBACK_DAYS,
                        "d1_window_start_unix": t0_d,
                        "d1_window_end_unix": t1_d,
                        "w1_window_start_unix": t0_w,
                        "w1_window_end_unix": t1_w,
                        "layer": layer,
                        "exported_at_utc": exported_at,
                    }
                )

            if n_z == 0:
                sheet_rows.append(
                    {
                        "symbol": symbol,
                        "ohlcv_source": source,
                        "zone_timeframe": "",
                        "zone_low": None,
                        "zone_high": None,
                        "price_mid": None,
                        "strength": None,
                        "fractal_count": 0,
                        "atr_d1_last": result.atr_d1_last,
                        "atr_w1_equiv": result.atr_w1_equiv,
                        "lookback_d1_days": HUMAN_LEVELS_D1_LOOKBACK_DAYS,
                        "lookback_w1_days": HUMAN_LEVELS_W1_LOOKBACK_DAYS,
                        "d1_window_start_unix": t0_d,
                        "d1_window_end_unix": t1_d,
                        "w1_window_start_unix": t0_w,
                        "w1_window_end_unix": t1_w,
                        "layer": layer,
                        "exported_at_utc": exported_at,
                        "note": "no_zones",
                    }
                )

            logger.info(
                "Human levels %s: zones_d1=%s zones_w1=%s",
                symbol,
                len(result.zones_d1),
                len(result.zones_w1),
            )
            time.sleep(0.05)
    finally:
        conn_atr.close()

    combined = pd.DataFrame(sheet_rows)
    if export_sheets and not HUMAN_LEVELS_DISABLE_SHEETS:
        export_human_levels_to_sheets(combined)
    elif export_sheets and HUMAN_LEVELS_DISABLE_SHEETS:
        logger.info("Human levels: Sheets export disabled by HUMAN_LEVELS_DISABLE_SHEETS")

    return symbols_with_zones, combined


__all__ = [
    "export_human_levels_to_sheets",
    "run_human_levels_batch_job",
]
