"""
Выгрузка в Google Sheets (MARKET_AUDIT_*): открытые позиции по текущему cycle_id,
дополняемая таблица закрытых сделок, а также открытые ордера.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)


def _repo_entrypoints() -> str:
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(root, "trading_bot", "entrypoints")


def _load_export_to_sheets():
    ep = _repo_entrypoints()
    if ep not in sys.path:
        sys.path.insert(0, ep)
    import export_to_sheets as es
    return es


def _exporter():
    es = _load_export_to_sheets()
    from trading_bot.tools.sheets_exporter import SheetsExporter
    return SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )


def _utc_iso(ts: Optional[int]) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return str(ts)


def sync_trading_positions_and_stats_to_sheets(cur) -> Dict[str, Any]:
    """
    1) Лист открытых позиций (pending/open) по trading_state.cycle_id — полная перезапись.
    2) Новые закрытые позиции — append в статистику (одна строка на position_record_id).
    """
    from trading_bot.config import settings as st

    out: Dict[str, Any] = {"ok": True, "skipped": None, "open_rows": 0, "stats_appended": 0}
    if os.getenv("PYTEST_CURRENT_TEST"):
        out["skipped"] = "pytest"
        return out
    if not st.SHEETS_TRADING_CYCLE_SYNC:
        out["skipped"] = "disabled"
        return out

    try:
        exporter = _exporter()
    except Exception as e:
        logger.warning("trading_cycle_sheets: exporter init failed: %s", e)
        out["ok"] = False
        out["error"] = str(e)
        return out

    ts_row = cur.execute("SELECT cycle_id FROM trading_state WHERE id = 1").fetchone()
    cycle_id = str(ts_row["cycle_id"]) if ts_row and ts_row["cycle_id"] else None

    open_cols: List[str] = [
        "exported_at_utc",
        "trading_cycle_id",
        "id",
        "uuid",
        "symbol",
        "side",
        "status",
        "qty",
        "filled_qty",
        "entry_price",
        "entry_price_fact",
        "opened_at_utc",
        "updated_at_utc",
        "structural_cycle_id",
        "entry_gate_confirmation_id",
        "entry_exec_order_id",
        "stop_exec_order_id",
        "exit_exec_order_id",
    ]
    open_rows: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    if cycle_id:
        rows = cur.execute(
            """
            SELECT * FROM position_records
            WHERE cycle_id = ? AND status IN ('pending', 'open')
            ORDER BY symbol, id
            """,
            (cycle_id,),
        ).fetchall()
        for r in rows:
            rd = dict(r)
            open_rows.append(
                {
                    "exported_at_utc": now_iso,
                    "trading_cycle_id": cycle_id,
                    "id": rd.get("id"),
                    "uuid": rd.get("uuid"),
                    "symbol": rd.get("symbol"),
                    "side": rd.get("side"),
                    "status": rd.get("status"),
                    "qty": rd.get("qty"),
                    "filled_qty": rd.get("filled_qty"),
                    "entry_price": rd.get("entry_price"),
                    "entry_price_fact": rd.get("entry_price_fact"),
                    "opened_at_utc": _utc_iso(rd.get("opened_at")),
                    "updated_at_utc": _utc_iso(rd.get("updated_at")),
                    "structural_cycle_id": rd.get("structural_cycle_id"),
                    "entry_gate_confirmation_id": rd.get("entry_gate_confirmation_id", ""),
                    "entry_exec_order_id": rd.get("entry_exec_order_id"),
                    "stop_exec_order_id": rd.get("stop_exec_order_id"),
                    "exit_exec_order_id": rd.get("exit_exec_order_id"),
                }
            )

    try:
        import pandas as pd
        df_open = pd.DataFrame(open_rows, columns=open_cols)
        es = _load_export_to_sheets()
        exporter.export_dataframe_to_sheet(
            df_open,
            es.SHEET_TITLE,
            st.CYCLE_OPEN_POSITIONS_WORKSHEET,
        )
        out["open_rows"] = len(df_open)
    except Exception:
        logger.exception("trading_cycle_sheets: open positions export failed")
        out["ok"] = False

    stats_headers: Sequence[str] = (
        "exported_at_utc",
        "cycle_id",
        "structural_cycle_id",
        "position_record_id",
        "symbol",
        "side",
        "qty",
        "filled_qty",
        "entry_price_fact",
        "exit_price_fact",
        "opened_at_utc",
        "closed_at_utc",
        "close_reason",
        "status",
        "meta_json_excerpt",
    )
    try:
        pending = cur.execute(
            """
            SELECT pr.id, pr.cycle_id, pr.structural_cycle_id, pr.symbol, pr.side, pr.qty, pr.filled_qty,
                   pr.entry_price_fact, pr.exit_price_fact, pr.opened_at, pr.closed_at, pr.close_reason,
                   pr.status, pr.meta_json
            FROM position_records pr
            LEFT JOIN sheet_stats_exported_position ex ON ex.position_record_id = pr.id
            WHERE pr.status = 'closed' AND ex.position_record_id IS NULL
            ORDER BY pr.closed_at ASC, pr.id ASC
            """
        ).fetchall()
        ws_name = st.CYCLE_TRADING_STATS_WORKSHEET
        ws = exporter._get_or_create_worksheet(ws_name)
        existing = ws.row_values(1)
        if not existing or not any(str(x).strip() for x in existing):
            ws.append_row(list(stats_headers), value_input_option="USER_ENTERED")

        appended = 0
        now_ts = int(time.time())
        for r in pending:
            rd = dict(r)
            meta_excerpt = ""
            mj = rd.get("meta_json")
            if mj:
                meta_excerpt = str(mj)[:500]
            row_vals = [
                now_iso,
                rd.get("cycle_id") or "",
                rd.get("structural_cycle_id") or "",
                rd.get("id"),
                rd.get("symbol") or "",
                rd.get("side") or "",
                rd.get("qty"),
                rd.get("filled_qty"),
                rd.get("entry_price_fact"),
                rd.get("exit_price_fact"),
                _utc_iso(rd.get("opened_at")),
                _utc_iso(rd.get("closed_at")),
                rd.get("close_reason") or "",
                rd.get("status") or "",
                meta_excerpt,
            ]
            ws.append_row([str(v) if v is not None else "" for v in row_vals], value_input_option="USER_ENTERED")
            cur.execute(
                "INSERT OR IGNORE INTO sheet_stats_exported_position (position_record_id, exported_at) VALUES (?, ?)",
                (int(rd["id"]), now_ts),
            )
            appended += 1
        out["stats_appended"] = appended
        if appended:
            logger.info("trading_cycle_sheets: stats sheet %s appended_rows=%s", ws_name, appended)
    except Exception:
        logger.exception("trading_cycle_sheets: stats append failed")
        out["ok"] = False

    logger.info(
        "trading_cycle_sheets: open_sheet=%s open_rows=%s stats_appended=%s",
        st.CYCLE_OPEN_POSITIONS_WORKSHEET,
        out.get("open_rows"),
        out.get("stats_appended"),
    )
    return out


# ========== НОВАЯ ФУНКЦИЯ: выгрузка открытых ордеров ==========
def export_open_orders_to_sheets(cur) -> Dict[str, Any]:
    """Выгружает все открытые (не filled/cancelled/rejected) ордера из exec_orders."""
    from trading_bot.config import settings as st
    out: Dict[str, Any] = {"ok": True, "rows": 0}
    if os.getenv("PYTEST_CURRENT_TEST"):
        out["skipped"] = "pytest"
        return out
    if not st.SHEETS_TRADING_CYCLE_SYNC:
        out["skipped"] = "disabled"
        return out

    try:
        exporter = _exporter()
    except Exception as e:
        logger.warning("trading_cycle_sheets: exporter init failed for open orders: %s", e)
        out["ok"] = False
        out["error"] = str(e)
        return out

    # Не закрытые ордера
    rows = cur.execute(
        """
        SELECT
            id, created_at, updated_at, cycle_id, structural_cycle_id,
            position_record_id, order_role, client_order_id, bybit_order_id,
            symbol, side, order_type, qty, price, status, exchange_status,
            filled_qty, avg_fill_price, reduce_only, error_message
        FROM exec_orders
        WHERE status NOT IN ('filled', 'cancelled', 'canceled', 'rejected', 'closed', 'failed', 'expired')
        ORDER BY created_at DESC
        """
    ).fetchall()

    if not rows:
        out["rows"] = 0
        out["skipped"] = "no_open_orders"
        return out

    import pandas as pd
    now_iso = datetime.now(timezone.utc).isoformat()
    data = []
    for r in rows:
        rd = dict(r)
        data.append({
            "exported_at_utc": now_iso,
            "id": rd.get("id"),
            "created_at_utc": _utc_iso(rd.get("created_at")),
            "updated_at_utc": _utc_iso(rd.get("updated_at")),
            "cycle_id": rd.get("cycle_id"),
            "structural_cycle_id": rd.get("structural_cycle_id"),
            "position_record_id": rd.get("position_record_id"),
            "order_role": rd.get("order_role"),
            "client_order_id": rd.get("client_order_id"),
            "bybit_order_id": rd.get("bybit_order_id"),
            "symbol": rd.get("symbol"),
            "side": rd.get("side"),
            "order_type": rd.get("order_type"),
            "qty": rd.get("qty"),
            "price": rd.get("price"),
            "status": rd.get("status"),
            "exchange_status": rd.get("exchange_status"),
            "filled_qty": rd.get("filled_qty"),
            "avg_fill_price": rd.get("avg_fill_price"),
            "reduce_only": rd.get("reduce_only"),
            "error_message": rd.get("error_message"),
        })
    df = pd.DataFrame(data)
    es = _load_export_to_sheets()
    ws_name = st.CYCLE_OPEN_ORDERS_WORKSHEET if hasattr(st, "CYCLE_OPEN_ORDERS_WORKSHEET") else "open_orders"
    exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws_name)
    out["rows"] = len(df)
    logger.info("export_open_orders_to_sheets: exported %s orders to %s", len(df), ws_name)
    return out


__all__ = ["sync_trading_positions_and_stats_to_sheets", "export_open_orders_to_sheets"]