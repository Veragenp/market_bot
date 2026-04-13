"""
Ручные глобальные уровни максимального горизонтального объёма → price_levels.

level_type = manual_global_hvn, origin = manual, layer фиксирован.
Ранг важности только в tier: \"1\" сильнее \"2\" сильнее \"3\" (меньше число = важнее).
strength и volume_peak для этого типа не используются (0 / NULL).
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Literal, Optional

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_STATUS_ACTIVE,
    LEVEL_STATUS_ARCHIVED,
    ORIGIN_MANUAL,
)

logger = logging.getLogger(__name__)

LEVEL_TYPE_MANUAL_GLOBAL_HVN = "manual_global_hvn"
LAYER_MANUAL_GLOBAL_HVN = "manual_global_hvn_sheet"

_TIER_RE = re.compile(r"^[1-9]\d{0,2}$")


@dataclass(frozen=True)
class ManualGlobalHvnRowParsed:
    stable_level_id: str
    price: float
    tier: str
    is_active: bool


def parse_manual_global_sheet_row(row: dict[str, Any]) -> ManualGlobalHvnRowParsed | None:
    """
    Разбор одной строки листа (ключи уже в lower).
    Возвращает None, если строка пустая / нет id / невалидные поля.
    """
    sid = _cell(row, "stable_level_id")
    if not sid:
        return None
    price_raw = _cell(row, "price")
    if price_raw is None or str(price_raw).strip() == "":
        logger.warning("manual_global_hvn: skip row (no price) stable_level_id=%s", sid)
        return None
    try:
        price = float(str(price_raw).replace(",", ".").strip())
    except ValueError:
        logger.warning("manual_global_hvn: skip row (bad price) stable_level_id=%s", sid)
        return None
    tier_raw = _cell(row, "tier")
    if not tier_raw:
        logger.warning("manual_global_hvn: skip row (no tier) stable_level_id=%s", sid)
        return None
    tier = str(tier_raw).strip()
    if not _TIER_RE.match(tier):
        logger.warning(
            "manual_global_hvn: skip row (tier must be 1–999) stable_level_id=%s tier=%s",
            sid,
            tier,
        )
        return None
    active_raw = _cell(row, "is_active")
    is_active = _parse_is_active(active_raw)
    return ManualGlobalHvnRowParsed(
        stable_level_id=sid.strip(),
        price=price,
        tier=tier,
        is_active=is_active,
    )


def _cell(row: dict[str, Any], key: str) -> str:
    v = row.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _parse_is_active(raw: str) -> bool:
    s = str(raw).strip().lower()
    if s in ("", "1", "true", "yes", "y", "да", "on"):
        return True
    if s in ("0", "false", "no", "n", "нет", "off"):
        return False
    return True


def upsert_manual_global_hvn_level(
    *,
    symbol: str,
    parsed: ManualGlobalHvnRowParsed,
    now_ts: Optional[int] = None,
) -> Literal["inserted", "updated", "skipped"]:
    init_db()
    run_migrations()
    ts = int(now_ts) if now_ts is not None else int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, level_type, origin
        FROM price_levels
        WHERE stable_level_id = ?
        """,
        (parsed.stable_level_id,),
    )
    existing = cur.fetchone()
    status = LEVEL_STATUS_ACTIVE if parsed.is_active else LEVEL_STATUS_ARCHIVED
    ia = 1 if parsed.is_active else 0

    if existing is not None:
        ex_sym = str(existing["symbol"])
        ex_lt = str(existing["level_type"])
        ex_or = str(existing["origin"])
        if ex_lt != LEVEL_TYPE_MANUAL_GLOBAL_HVN or ex_or != ORIGIN_MANUAL:
            logger.error(
                "manual_global_hvn: stable_level_id=%s already used by another type (%s/%s); skip",
                parsed.stable_level_id,
                ex_lt,
                ex_or,
            )
            conn.close()
            return "skipped"
        if ex_sym != symbol:
            logger.error(
                "manual_global_hvn: stable_level_id=%s belongs to symbol=%s but sheet is %s; skip",
                parsed.stable_level_id,
                ex_sym,
                symbol,
            )
            conn.close()
            return "skipped"
        cur.execute(
            """
            UPDATE price_levels
            SET price = ?,
                tier = ?,
                is_active = ?,
                status = ?,
                strength = 0,
                volume_peak = NULL,
                duration_hours = NULL,
                t_start_unix = NULL,
                t_end_unix = NULL,
                updated_at = ?,
                last_matched_calc_at = ?
            WHERE stable_level_id = ?
              AND level_type = ?
              AND origin = ?
              AND symbol = ?
            """,
            (
                float(parsed.price),
                parsed.tier,
                ia,
                status,
                ts,
                ts,
                parsed.stable_level_id,
                LEVEL_TYPE_MANUAL_GLOBAL_HVN,
                ORIGIN_MANUAL,
                symbol,
            ),
        )
        conn.commit()
        conn.close()
        return "updated"

    try:
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, layer,
                origin, status, stable_level_id,
                strength, volume_peak, tier,
                duration_hours, t_start_unix, t_end_unix,
                lookback_days, timeframe,
                created_at, updated_at, last_matched_calc_at,
                expires_at, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, NULL, ?)
            """,
            (
                symbol,
                float(parsed.price),
                LEVEL_TYPE_MANUAL_GLOBAL_HVN,
                LAYER_MANUAL_GLOBAL_HVN,
                ORIGIN_MANUAL,
                status,
                parsed.stable_level_id,
                parsed.tier,
                ts,
                ts,
                ts,
                ia,
            ),
        )
    except sqlite3.IntegrityError as e:
        logger.error(
            "manual_global_hvn: INSERT failed (duplicate stable_level_id?): %s — %s",
            parsed.stable_level_id,
            e,
        )
        conn.rollback()
        conn.close()
        return "skipped"
    conn.commit()
    conn.close()
    return "inserted"


__all__ = [
    "LAYER_MANUAL_GLOBAL_HVN",
    "LEVEL_TYPE_MANUAL_GLOBAL_HVN",
    "ManualGlobalHvnRowParsed",
    "parse_manual_global_sheet_row",
    "upsert_manual_global_hvn_level",
]
