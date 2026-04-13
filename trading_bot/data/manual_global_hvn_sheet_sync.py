"""
Синхронизация ручных глобальных HVN из Google Sheets → SQLite.

По умолчанию берёт объединение:
- TRADING_SYMBOLS
- ANALYTIC_SYMBOLS["crypto_context"] (и fallback ANALYTIC_SYMBOLS["crypto"])
- ANALYTIC_SYMBOLS["macro"]
- ANALYTIC_SYMBOLS["indices"]
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from trading_bot.config.settings import (
    GOOGLE_CREDENTIALS_PATH,
    REPO_ROOT,
    MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET,
    MANUAL_GLOBAL_HVN_SPREADSHEET_ID,
    MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE,
    MANUAL_GLOBAL_HVN_SPREADSHEET_URL,
)
from trading_bot.config.symbols import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.data.manual_global_hvn_db import (
    parse_manual_global_sheet_row,
    upsert_manual_global_hvn_level,
)
from trading_bot.tools.sheets_reader import DEFAULT_HEADER_ROW, SheetsReader, worksheet_title_for_symbol

logger = logging.getLogger(__name__)


def default_manual_global_hvn_symbols() -> list[str]:
    """
    Базовый набор символов для ручных глобальных уровней:
    trading + crypto_context + macro + indices (без дублей, в стабильном порядке).
    """
    out: list[str] = []
    seen: set[str] = set()
    for s in TRADING_SYMBOLS:
        if s not in seen:
            seen.add(s)
            out.append(s)
    for s in ANALYTIC_SYMBOLS.get("crypto_context", []):
        if s not in seen:
            seen.add(s)
            out.append(s)
    # Backward compatibility with older key name.
    for s in ANALYTIC_SYMBOLS.get("crypto", []):
        if s not in seen:
            seen.add(s)
            out.append(s)
    for s in ANALYTIC_SYMBOLS.get("macro", []):
        if s not in seen:
            seen.add(s)
            out.append(s)
    for s in ANALYTIC_SYMBOLS.get("indices", []):
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _resolve_credentials_path() -> str:
    p = GOOGLE_CREDENTIALS_PATH.strip()
    if os.path.isabs(p):
        return p
    return os.path.normpath(os.path.join(REPO_ROOT, p))


def _instruction_titles() -> set[str]:
    raw = (MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET or "instruction").strip()
    parts = {raw.lower()}
    for p in raw.split(","):
        p = p.strip().lower()
        if p:
            parts.add(p)
    return parts


def open_reader() -> SheetsReader:
    cred = _resolve_credentials_path()
    sid = (MANUAL_GLOBAL_HVN_SPREADSHEET_ID or "").strip()
    surl = (MANUAL_GLOBAL_HVN_SPREADSHEET_URL or "").strip()
    stitle = (MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE or "").strip()
    if sid:
        return SheetsReader(cred, spreadsheet_id=sid)
    if surl:
        return SheetsReader(cred, spreadsheet_url=surl)
    if stitle:
        return SheetsReader(cred, spreadsheet_title=stitle)
    raise RuntimeError(
        "Set MANUAL_GLOBAL_HVN_SPREADSHEET_ID, MANUAL_GLOBAL_HVN_SPREADSHEET_URL, "
        "or MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE in .env"
    )


def sync_manual_global_hvn_from_sheets(
    *,
    symbols: Optional[list[str]] = None,
    dry_run: bool = False,
    ensure_tabs: bool = True,
) -> dict[str, Any]:
    """
    Для каждого символа: вкладка = worksheet_title_for_symbol(symbol);
    читает строки, upsert в price_levels.

    ensure_tabs: создать лист с шапкой, если отсутствует.
    """
    syms = list(symbols) if symbols is not None else default_manual_global_hvn_symbols()
    reader = open_reader()
    instruction_names = _instruction_titles()
    known_tabs = set(reader.list_worksheet_titles())
    stats: dict[str, Any] = {
        "spreadsheet_title": reader.spreadsheet_title,
        "symbols_total": len(syms),
        "rows_parsed": 0,
        "inserted": 0,
        "updated": 0,
        "skipped_rows": 0,
        "skipped_symbols": [],
        "tabs_created": [],
        "errors": [],
    }

    for symbol in syms:
        title = worksheet_title_for_symbol(symbol)
        if title.strip().lower() in instruction_names:
            logger.warning("Skip symbol tab conflicting with instruction sheet name: %s", title)
            continue
        try:
            if ensure_tabs and title not in known_tabs:
                # Avoid extra metadata read per symbol: we already know the tab is missing.
                reader.get_or_create_worksheet(
                    title,
                    header_row=DEFAULT_HEADER_ROW,
                    assume_missing=True,
                )
                known_tabs.add(title)
                stats["tabs_created"].append(title)
            rows = reader.read_worksheet_dicts(title)
        except Exception as e:
            msg = f"{symbol} ({title}): {e}"
            logger.exception("manual_global_hvn sync failed for %s", symbol)
            stats["errors"].append(msg)
            stats["skipped_symbols"].append(symbol)
            continue

        for raw in rows:
            parsed = parse_manual_global_sheet_row(raw)
            if parsed is None:
                stats["skipped_rows"] += 1
                continue
            stats["rows_parsed"] += 1
            if dry_run:
                continue
            res = upsert_manual_global_hvn_level(symbol=symbol, parsed=parsed)
            if res == "inserted":
                stats["inserted"] += 1
            elif res == "updated":
                stats["updated"] += 1
            else:
                stats["skipped_rows"] += 1

    return stats


__all__ = [
    "default_manual_global_hvn_symbols",
    "open_reader",
    "sync_manual_global_hvn_from_sheets",
]
