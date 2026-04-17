from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from trading_bot.config import settings as st

logger = logging.getLogger(__name__)

_SHEET_HEADERS = (
    "ts_utc",
    "run_id",
    "cycle_id",
    "stage",
    "status",
    "severity",
    "message",
    "duration_ms",
    "details_json",
)


def _fmt_message(stage: str, status: str, cycle_id: Optional[str], message: str) -> str:
    short = (cycle_id or "")[:8]
    return f"[{stage}] {status} cycle={short or '-'} {message}".strip()


def _append_sheet_row(
    *,
    ts: int,
    run_id: Optional[str],
    cycle_id: Optional[str],
    stage: str,
    status: str,
    severity: Optional[str],
    message: str,
    duration_ms: Optional[int],
    details: Optional[Dict[str, Any]],
) -> None:
    if not st.OPS_STAGE_SHEETS or os.getenv("PYTEST_CURRENT_TEST"):
        return
    try:
        from trading_bot.tools.sheets_exporter import SheetsExporter
        from trading_bot.entrypoints import export_to_sheets as es

        exporter = SheetsExporter(
            credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
            spreadsheet_title=es.SHEET_TITLE,
            spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
            spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
        )
        ws = exporter._get_or_create_worksheet(st.OPS_STAGE_WORKSHEET)
        if not ws.get_all_values():
            ws.append_row(list(_SHEET_HEADERS), value_input_option="USER_ENTERED")
        exporter.append_row(
            st.OPS_STAGE_WORKSHEET,
            [
                datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                run_id or "",
                cycle_id or "",
                stage,
                status,
                severity or "",
                message,
                "" if duration_ms is None else int(duration_ms),
                json.dumps(details or {}, ensure_ascii=False),
            ],
        )
    except Exception:
        logger.exception("ops_stage: append sheet row failed")


def _send_telegram(stage: str, status: str, cycle_id: Optional[str], message: str, severity: Optional[str]) -> None:
    if not st.OPS_STAGE_TELEGRAM or os.getenv("PYTEST_CURRENT_TEST"):
        return
    
    # Отключаем спам от рутинных тиков supervisor
    # Эти этапы вызываются каждый тик и не несут полезной информации для Telegram
    if stage in ("ENTRY_TICK", "ENTRY_SIGNAL", "ORDER_RECONCILE", "DATA_REFRESH", "LEVELS_REBUILD"):
        # Отправляем только если есть ошибка
        if (severity or "").lower() not in ("error", "critical"):
            return
    
    if st.OPS_STAGE_TELEGRAM_ONLY_FINAL and status == "started":
        return
    if st.OPS_STAGE_TELEGRAM_ONLY_FINAL and (severity or "").lower() not in ("error", "critical") and status not in (
        "ok",
        "failed",
    ):
        return
    try:
        from trading_bot.tools.telegram_notify import escape_html_telegram, send_telegram_message

        text = _fmt_message(stage, status, cycle_id, message)
        send_telegram_message(f"<pre>{escape_html_telegram(text)}</pre>", parse_mode="HTML")
    except Exception:
        logger.exception("ops_stage: telegram send failed")


def record_stage_event(
    cur,
    *,
    stage: str,
    status: str,
    cycle_id: Optional[str] = None,
    run_id: Optional[str] = None,
    severity: Optional[str] = None,
    message: str = "",
    details: Optional[Dict[str, Any]] = None,
    started_at: Optional[int] = None,
    finished_at: Optional[int] = None,
) -> None:
    ts = int(finished_at or started_at or datetime.now(timezone.utc).timestamp())
    duration_ms = None
    if started_at is not None and finished_at is not None and finished_at >= started_at:
        duration_ms = int((finished_at - started_at) * 1000)
    cur.execute(
        """
        INSERT INTO ops_stage_runs (
            run_id, cycle_id, stage, status, severity, message, details_json,
            started_at, finished_at, duration_ms, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            cycle_id,
            stage,
            status,
            severity,
            message,
            json.dumps(details, ensure_ascii=False) if details else None,
            started_at,
            finished_at,
            duration_ms,
            ts,
        ),
    )
    if st.OPS_STAGE_LOG:
        logger.info("%s", _fmt_message(stage, status, cycle_id, message))
    _append_sheet_row(
        ts=ts,
        run_id=run_id,
        cycle_id=cycle_id,
        stage=stage,
        status=status,
        severity=severity,
        message=message,
        duration_ms=duration_ms,
        details=details,
    )
    _send_telegram(stage, status, cycle_id, message, severity)


__all__ = ["record_stage_event"]
