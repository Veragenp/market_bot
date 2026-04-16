"""
Операционные уведомления structural-контура: структурный лог, Telegram, Google Sheets.

Не смешивается с торговыми сигналами входа — только состояние скана / realtime / freeze.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_OPS_LOG_HEADERS = (
    "ts_utc",
    "cycle_id",
    "event_type",
    "symbol",
    "price",
    "meta_json",
    "message",
)


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_export_to_sheets():
    _REPO = _repo_root()
    _EP = os.path.join(_REPO, "trading_bot", "entrypoints")
    if _EP not in sys.path:
        sys.path.insert(0, _EP)
    import export_to_sheets as es  # noqa: E402

    return es


def _load_params_json(cur, cycle_id: str) -> dict:
    row = cur.execute("SELECT params_json FROM structural_cycles WHERE id = ?", (cycle_id,)).fetchone()
    if not row or not row["params_json"]:
        return {}
    try:
        return json.loads(row["params_json"])
    except json.JSONDecodeError:
        return {}


def _build_levels_dataframe(cycle_id: str, pipeline_out: Optional[Dict[str, Any]] = None) -> Any:
    import pandas as pd

    from trading_bot.data.db import get_connection

    pipeline_out = pipeline_out or {}
    exported_at = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    cur = conn.cursor()
    pj = _load_params_json(cur, cycle_id)
    sc = cur.execute(
        """
        SELECT phase, pool_median_w, pool_mad, pool_k, symbols_valid_count, cancel_reason
        FROM structural_cycles WHERE id = ?
        """,
        (cycle_id,),
    ).fetchone()
    rows = cur.execute(
        """
        SELECT
            symbol, status,
            level_below_id, level_above_id,
            L_price, U_price, atr, W_atr,
            ref_price_ws, mid_price, mid_band_low, mid_band_high,
            volume_peak_below, volume_peak_above,
            tier_below, tier_above
        FROM structural_cycle_symbols
        WHERE cycle_id = ?
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    conn.close()

    logger.debug("Building levels dataframe for cycle_id=%s, rows=%d", cycle_id, len(rows))

    ref_src = pipeline_out.get("ref_price_source")
    pool_m_w = float(pipeline_out.get("pool_median_w") or 0.0)
    pool_mad_w = float(pipeline_out.get("pool_mad") or 0.0)
    pool_m_r = float(pipeline_out.get("pool_median_r") or 0.0)
    pool_mad_r = float(pipeline_out.get("pool_mad_r") or 0.0)
    mad_k = float(
        (sc["pool_k"] if sc and sc["pool_k"] is not None else pj.get("z_w_ok_threshold", 1.0)) or 1.0
    )
    center_k = float(pj.get("center_mad_k") or 0.0)
    center_enabled = bool(pj.get("center_filter_enabled", False))
    records: List[Dict[str, Any]] = []
    for r in rows:
        lp = r["L_price"]
        up = r["U_price"]
        atr = r["atr"]
        refp = r["ref_price_ws"]
        w_atr = r["W_atr"]
        center_ratio = (
            (float(refp) - float(lp)) / (float(up) - float(lp))
            if refp is not None and lp is not None and up is not None and float(up) > float(lp)
            else None
        )
        abs_w = abs(float(w_atr) - pool_m_w) if w_atr is not None else None
        z_w = (abs_w / pool_mad_w) if abs_w is not None and pool_mad_w > 1e-9 else None
        abs_r = abs(float(center_ratio) - pool_m_r) if center_ratio is not None else None
        z_r = (abs_r / pool_mad_r) if abs_r is not None and pool_mad_r > 1e-9 else None
        ok_w = (1 if z_w <= mad_k else 0) if z_w is not None else None
        ok_r = (1 if z_r <= center_k else 0) if (center_enabled and z_r is not None) else None
        if ok_w is None:
            tier_hint = None
        elif center_enabled and ok_r is not None:
            tier_hint = "ok_w_and_ok_r" if (ok_w == 1 and ok_r == 1) else ("ok_w_only" if ok_w == 1 else "fallback_any")
        else:
            tier_hint = "ok_w_only" if ok_w == 1 else "fallback_any"
        width_price = (float(up) - float(lp)) if lp is not None and up is not None else None
        dist_l = (abs(float(refp) - float(lp)) / float(atr)) if refp and lp and atr else None
        dist_u = (abs(float(up) - float(refp)) / float(atr)) if refp and up and atr else None
        records.append(
            {
                "exported_at_utc": exported_at,
                "structural_cycle_id": cycle_id,
                "ref_price_source": ref_src,
                "scan_phase": sc["phase"] if sc else None,
                "cancel_reason": sc["cancel_reason"] if sc else None,
                "pool_median_W_atr": sc["pool_median_w"] if sc else None,
                "pool_MAD_W_atr": sc["pool_mad"] if sc else None,
                "pool_median_center_ratio": pipeline_out.get("pool_median_r"),
                "pool_MAD_center_ratio": pipeline_out.get("pool_mad_r"),
                "pool_MAD_k": sc["pool_k"] if sc else None,
                "center_MAD_k": pj.get("center_mad_k"),
                "center_filter_enabled": pj.get("center_filter_enabled"),
                "target_align_enabled": pj.get("target_align_enabled"),
                "anchor_symbols": ",".join(pj.get("anchor_symbols") or []),
                "target_w_band_k": pj.get("target_w_band_k"),
                "target_center_weight": pj.get("target_center_weight"),
                "target_width_weight": pj.get("target_width_weight"),
                "symbols_ok_in_pool": sc["symbols_valid_count"] if sc else None,
                "min_pool_required": pj.get("min_pool_symbols"),
                "allowed_level_types": ",".join(pj.get("allowed_level_types") or []),
                "symbol": r["symbol"],
                "row_status": r["status"],
                "ref_price": refp,
                "long_L_strongest_below": lp,
                "short_U_strongest_above": up,
                "level_below_id": r["level_below_id"],
                "level_above_id": r["level_above_id"],
                "volume_peak_below": r["volume_peak_below"],
                "volume_peak_above": r["volume_peak_above"],
                "tier_below": r["tier_below"],
                "tier_above": r["tier_above"],
                "atr_daily": atr,
                "corridor_width_atr": w_atr,
                "center_ratio_ref_in_corridor": center_ratio,
                "z_w": z_w,
                "z_r": z_r if center_enabled else None,
                "ok_w": ok_w,
                "ok_r": ok_r if center_enabled else None,
                "chosen_tier_hint": tier_hint,
                "corridor_width_price": width_price,
                "mid_price": r["mid_price"],
                "mid_band_low": r["mid_band_low"],
                "mid_band_high": r["mid_band_high"],
                "dist_ref_to_L_atr": dist_l,
                "dist_ref_to_U_atr": dist_u,
            }
        )
    df = pd.DataFrame(records)
    logger.info("Built levels dataframe with %d records for cycle_id=%s", len(df), cycle_id)
    return df


def build_structural_trading_levels_df(cycle_id: str) -> Any:
    """
    Новая функция: формирует упрощённый DataFrame только с ТОРГОВЫМИ уровнями
    для выгрузки в Google Sheets. Включает: символ, направление, цену уровня,
    текущую цену, расстояние в ATR, объём пика.
    """
    import pandas as pd

    from trading_bot.data.db import get_connection
    from trading_bot.tools.price_feed import PricePoint, get_price_feed

    conn = get_connection()
    cur = conn.cursor()
    
    # Загружаем structural_cycle_symbols
    sc_rows = cur.execute(
        """
        SELECT symbol, L_price, U_price, atr, ref_price_ws,
               tier_below, tier_above, volume_peak_below, volume_peak_above
        FROM structural_cycle_symbols
        WHERE cycle_id = ? AND status = 'ok'
        """,
        (cycle_id,),
    ).fetchall()
    
    # Получаем текущие цены
    symbols = [str(r["symbol"]) for r in sc_rows]
    feed = get_price_feed()
    live_prices = feed.get_prices(symbols)
    
    records = []
    for r in sc_rows:
        sym = str(r["symbol"])
        ref_price = float(r["ref_price_ws"]) if r["ref_price_ws"] else None
        atr = float(r["atr"]) if r["atr"] else None
        
        # Текущая цена
        live_price = live_prices.get(sym)
        current_price = float(live_price.price) if live_price else (ref_price or 0.0)
        
        # LONG уровень
        if r["L_price"] is not None:
            lp = float(r["L_price"])
            dist_atr = abs(lp - current_price) / atr if atr and atr > 0 else None
            records.append({
                "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "structural_cycle_id": cycle_id,
                "symbol": sym,
                "direction": "LONG",
                "level_price": lp,
                "tier": r["tier_below"],
                "volume_peak": r["volume_peak_below"],
                "current_price": current_price,
                "dist_from_current_atr": dist_atr,
                "atr": atr,
                "ref_price": ref_price,
            })
        
        # SHORT уровень
        if r["U_price"] is not None:
            up = float(r["U_price"])
            dist_atr = abs(up - current_price) / atr if atr and atr > 0 else None
            records.append({
                "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "structural_cycle_id": cycle_id,
                "symbol": sym,
                "direction": "SHORT",
                "level_price": up,
                "tier": r["tier_above"],
                "volume_peak": r["volume_peak_above"],
                "current_price": current_price,
                "dist_from_current_atr": dist_atr,
                "atr": atr,
                "ref_price": ref_price,
            })
    
    conn.close()
    
    df = pd.DataFrame(records)
    if not df.empty:
        # Сортировка: сначала направление, потом символ
        df = df.sort_values(["direction", "symbol"]).reset_index(drop=True)
    
    logger.info("Built trading levels dataframe with %d records for cycle_id=%s", len(df), cycle_id)
    return df


def export_levels_snapshot(cycle_id: str, pipeline_out: Optional[Dict[str, Any]] = None) -> None:
    from trading_bot.config import settings as st
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        logger.debug("STRUCTURAL_OPS_SHEETS_LEVELS disabled, skipping export")
        return
    es = _load_export_to_sheets()
    df = _build_levels_dataframe(cycle_id, pipeline_out)
    if df.empty:
        logger.warning("Empty dataframe for levels snapshot, skipping export")
        return
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws = os.getenv("STRUCTURAL_LEVELS_REPORT_WORKSHEET", st.STRUCTURAL_LEVELS_REPORT_WORKSHEET)
    logger.info("Exporting levels snapshot to sheet '%s', rows=%d, cycle_id=%s", ws, len(df), cycle_id)
    
    # Логирование текущих цен и уровней для каждого символа
    try:
        logger.info("Levels snapshot by symbol:")
        for sym in sorted(df["symbol"].unique()):
            sym_df = df[df["symbol"] == sym].iloc[0]
            ref_price = sym_df.get("ref_price")
            l_price = sym_df.get("long_L_strongest_below")
            u_price = sym_df.get("short_U_strongest_above")
            status = sym_df.get("row_status")
            
            level_info = []
            if l_price is not None:
                level_info.append(f"L:{l_price:.2f}")
            if u_price is not None:
                level_info.append(f"U:{u_price:.2f}")
            
            if ref_price:
                logger.info("  %s: ref=%.2f | levels=[%s] status=%s", sym, ref_price, ", ".join(level_info), status)
            else:
                logger.info("  %s: levels=[%s] status=%s", sym, ", ".join(level_info), status)
    except Exception:
        logger.exception("Failed to log levels snapshot")
    
    try:
        exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws)
        logger.info("Successfully exported levels snapshot to %s", ws)
    except Exception as e:
        logger.exception("Failed to export levels snapshot to sheet %s", ws)
        raise


def export_structural_trading_levels(cycle_id: str) -> None:
    """
    Экспорт упрощённой таблицы ТОРГОВЫХ уровней в Google Sheets.
    Лист: structural_trading_levels (или из env STRUCTURAL_TRADING_LEVELS_WORKSHEET)
    """
    from trading_bot.config import settings as st
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        logger.debug("STRUCTURAL_OPS_SHEETS_LEVELS disabled, skipping export")
        return
    
    es = _load_export_to_sheets()
    df = build_structural_trading_levels_df(cycle_id)
    if df.empty:
        logger.warning("Empty trading levels dataframe for cycle_id=%s, skipping export", cycle_id)
        return
    
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    
    ws_name = os.getenv("STRUCTURAL_TRADING_LEVELS_WORKSHEET", "structural_trading_levels")
    logger.info("Exporting trading levels to sheet '%s', rows=%d, cycle_id=%s", ws_name, len(df), cycle_id)
    
    # Логирование текущих цен для каждого символа
    try:
        logger.info("Current prices by symbol:")
        price_summary = df.groupby("symbol").agg({
            "current_price": "first",
            "level_price": list,
            "direction": list
        }).reset_index()
        
        for _, row in price_summary.iterrows():
            sym = row["symbol"]
            current = row["current_price"]
            levels = row["level_price"]
            directions = row["direction"]
            level_str = ", ".join([f"{d}:{p:.2f}" for d, p in zip(directions, levels)])
            logger.info("  %s: current=%.2f | levels=[%s]", sym, current, level_str)
    except Exception:
        logger.exception("Failed to log price summary")
    
    try:
        exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws_name)
        logger.info("Successfully exported trading levels to %s", ws_name)
    except Exception:
        logger.exception("Failed to export trading levels to sheet %s", ws_name)


def export_levels_snapshot_v2() -> None:
    from trading_bot.analytics.structural_cycle_v2 import build_structural_v2_report_df
    from trading_bot.config import settings as st
    from trading_bot.data.db import get_connection
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        return
    es = _load_export_to_sheets()
    conn = get_connection()
    try:
        cur = conn.cursor()
        df = build_structural_v2_report_df(cur)
    finally:
        conn.close()
    if df.empty:
        logger.warning("Empty dataframe for levels v2 snapshot, skipping")
        return
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws = os.getenv("STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET", st.STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET)
    logger.info("Exporting levels v2 snapshot to sheet '%s', rows=%d", ws, len(df))
    try:
        exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws)
        logger.info("Successfully exported levels v2 snapshot")
    except Exception as e:
        logger.exception("Failed to export levels v2 snapshot")


def export_levels_snapshot_v3() -> None:
    from trading_bot.analytics.structural_cycle_v3 import build_structural_v3_report_df
    from trading_bot.config import settings as st
    from trading_bot.data.db import get_connection
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        return
    es = _load_export_to_sheets()
    conn = get_connection()
    try:
        cur = conn.cursor()
        df = build_structural_v3_report_df(cur)
    finally:
        conn.close()
    if df.empty:
        logger.warning("Empty dataframe for levels v3 snapshot, skipping")
        return
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws = os.getenv("STRUCTURAL_LEVELS_REPORT_V3_WORKSHEET", st.STRUCTURAL_LEVELS_REPORT_V3_WORKSHEET)
    logger.info("Exporting levels v3 snapshot to sheet '%s', rows=%d", ws, len(df))
    try:
        exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws)
        logger.info("Successfully exported levels v3 snapshot")
    except Exception as e:
        logger.exception("Failed to export levels v3 snapshot")


def export_levels_snapshot_v4() -> None:
    from trading_bot.analytics.structural_cycle_v4 import build_structural_v4_report_df
    from trading_bot.config import settings as st
    from trading_bot.data.db import get_connection
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        return
    es = _load_export_to_sheets()
    conn = get_connection()
    try:
        cur = conn.cursor()
        df = build_structural_v4_report_df(cur)
    finally:
        conn.close()
    if df.empty:
        logger.warning("Empty dataframe for levels v4 snapshot, skipping")
        return
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws = os.getenv("STRUCTURAL_LEVELS_REPORT_V4_WORKSHEET", st.STRUCTURAL_LEVELS_REPORT_V4_WORKSHEET)
    logger.info("Exporting levels v4 snapshot to sheet '%s', rows=%d", ws, len(df))
    try:
        exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws)
        logger.info("Successfully exported levels v4 snapshot")
    except Exception as e:
        logger.exception("Failed to export levels v4 snapshot")


def _human_message(
    event_type: str,
    cycle_id: str,
    symbol: Optional[str],
    price: Optional[float],
    meta: Optional[Dict[str, Any]],
) -> str:
    meta = meta or {}
    short_id = cycle_id[:8] if cycle_id else ""
    if event_type == "phase_change":
        if meta.get("action") == "freeze":
            nrows = meta.get("cycle_levels_rows")
            return f"Structural [{short_id}]: уровни для торговли зафиксированы (freeze), строк в cycle_levels: {nrows}"
        to = meta.get("to")
        if to == "scanning":
            return f"Structural [{short_id}]: старт цикла, фаза scanning"
        if to == "touch_window":
            return f"Structural [{short_id}]: realtime — окно касаний mid-полосы (touch_window)"
        if to == "entry_timer":
            n = meta.get("touch_count")
            return (
                f"Structural [{short_id}]: касание середины — "
                f"разных монет в окне достаточно ({n}), фаза entry_timer"
            )
        if to == "armed":
            return f"Structural [{short_id}]: пул готов, фаза armed"
        if to == "cancelled":
            reason = meta.get("reason") or meta.get("cancel_reason") or "unknown"
            return f"Structural [{short_id}]: отмена цикла, причина: {reason}"
        return f"Structural [{short_id}]: phase_change → {to} {meta}"
    if event_type == "breakout_lower":
        return (
            f"Structural [{short_id}]: пробой LONG-уровня (L) {symbol or '?'} "
            f"price={price}"
        )
    if event_type == "breakout_upper":
        return (
            f"Structural [{short_id}]: пробой SHORT-уровня (U) {symbol or '?'} "
            f"price={price}"
        )
    if event_type == "mid_touch":
        return f"Structural [{short_id}]: mid_touch {symbol or '?'} price={price}"
    return f"Structural [{short_id}]: {event_type} sym={symbol} price={price} meta={meta}"


def _should_telegram_event(
    event_type: str,
    meta: Optional[Dict[str, Any]],
) -> bool:
    from trading_bot.config import settings as st

    if not st.STRUCTURAL_OPS_TELEGRAM:
        return False
    if event_type in ("breakout_lower", "breakout_upper"):
        return True
    if event_type == "mid_touch":
        return st.STRUCTURAL_OPS_TELEGRAM_EACH_MID_TOUCH
    if event_type == "phase_change":
        if (meta or {}).get("action") == "freeze":
            return True
        to = (meta or {}).get("to")
        if to in ("scanning", "touch_window", "entry_timer", "armed", "cancelled"):
            return True
    return False


def _append_ops_sheet_row(
    cycle_id: str,
    event_type: str,
    ts: int,
    symbol: Optional[str],
    price: Optional[float],
    meta: Optional[Dict[str, Any]],
    message: str,
) -> None:
    from trading_bot.config import settings as st
    from trading_bot.tools.sheets_exporter import SheetsExporter

    if not st.STRUCTURAL_OPS_SHEETS_LOG or os.getenv("PYTEST_CURRENT_TEST"):
        return
    es = _load_export_to_sheets()
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws_name = st.STRUCTURAL_OPS_LOG_WORKSHEET
    gws = exporter._get_or_create_worksheet(ws_name)
    if not gws.get_all_values():
        gws.append_row(list(_OPS_LOG_HEADERS), value_input_option="USER_ENTERED")
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    exporter.append_row(
        ws_name,
        [
            ts_iso,
            cycle_id,
            event_type,
            symbol or "",
            "" if price is None else price,
            json.dumps(meta, ensure_ascii=False) if meta else "",
            message,
        ],
    )


def on_structural_event(
    cycle_id: str,
    event_type: str,
    ts: int,
    *,
    symbol: Optional[str] = None,
    price: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    from trading_bot.config import settings as st
    from trading_bot.tools.telegram_notify import escape_html_telegram, send_telegram_message

    msg = _human_message(event_type, cycle_id, symbol, price, meta)
    if os.getenv("PYTEST_CURRENT_TEST"):
        if st.STRUCTURAL_OPS_LOG:
            logger.info("%s", msg)
        return
    if st.STRUCTURAL_OPS_LOG:
        logger.info("%s", msg)
    if not cycle_id:
        return
    sheet_mid_ok = st.STRUCTURAL_OPS_SHEETS_LOG_EACH_MID_TOUCH or event_type != "mid_touch"
    if st.STRUCTURAL_OPS_SHEETS_LOG and sheet_mid_ok:
        try:
            _append_ops_sheet_row(cycle_id, event_type, ts, symbol, price, meta, msg)
        except Exception:
            logger.exception("Structural ops: append ops log sheet failed")
    if _should_telegram_event(event_type, meta):
        try:
            send_telegram_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")
        except Exception:
            logger.exception("Structural ops: telegram send failed")


def notify_no_valid_ref_prices(
    *,
    ref_source: str,
    symbols_requested: int,
    symbols_with_ref: int,
) -> None:
    from trading_bot.config import settings as st
    from trading_bot.tools.telegram_notify import escape_html_telegram, send_telegram_message

    msg = (
        f"Structural: нет валидных ref-цен — скан не запущен "
        f"(источник={ref_source}, запрошено символов={symbols_requested}, с ценой>0={symbols_with_ref})"
    )
    if st.STRUCTURAL_OPS_LOG:
        logger.warning("%s", msg)
    if st.STRUCTURAL_OPS_TELEGRAM and not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            send_telegram_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")
        except Exception:
            logger.exception("Structural ops: telegram send failed (no ref)")


__all__ = [
    "export_levels_snapshot",
    "export_structural_trading_levels",
    "export_levels_snapshot_v2",
    "export_levels_snapshot_v3",
    "notify_no_valid_ref_prices",
    "on_structural_event",
]