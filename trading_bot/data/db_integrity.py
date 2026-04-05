"""
Проверки SQLite после смены пути к БД, миграций и загрузчиков.

Использование:
  from trading_bot.data.db_integrity import run_db_integrity_checks
  results, ok = run_db_integrity_checks(strict=False)

CLI: python -m trading_bot.scripts.check_db_integrity [--strict]
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from trading_bot.config.settings import DB_PATH
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations

MIN_SCHEMA_VERSION = 9

# Мягкие пороги (strict=False): только явные поломки схемы / пустота критичных мест
MIN_OHLCV_1D_BARS_BINANCE = 1
MIN_OHLCV_1D_BARS_SAMPLE = 5  # хотя бы у части спота

# Строгие: данные должны быть свежими (секунды)
STRICT_MAX_AGE_1M_BINANCE = 48 * 3600
STRICT_MAX_AGE_1D_BINANCE = 7 * 86400
STRICT_MIN_INSTRUMENTS_WITH_ATR = 1


@dataclass
class DbCheckResult:
    name: str
    ok: bool
    detail: str
    required: bool  # если False — только предупреждение


def _cursor() -> Any:
    init_db()
    run_migrations()
    conn = get_connection()
    return conn, conn.cursor()


def check_pragma_integrity() -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute("PRAGMA integrity_check")
        row = cur.fetchone()
        val = row[0] if row else ""
        ok = val == "ok"
        return DbCheckResult("pragma_integrity_check", ok, str(val), True)
    finally:
        conn.close()


def check_schema_version() -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute("SELECT MAX(version) FROM db_version")
        row = cur.fetchone()
        ver = int(row[0]) if row and row[0] is not None else 0
        ok = ver >= MIN_SCHEMA_VERSION
        return DbCheckResult(
            "schema_version",
            ok,
            f"db_version={ver} (min {MIN_SCHEMA_VERSION}), file={DB_PATH}",
            True,
        )
    finally:
        conn.close()


def check_active_levels_stable_id() -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM price_levels
            WHERE is_active = 1
              AND (stable_level_id IS NULL OR trim(stable_level_id) = '')
            """
        )
        n = int(cur.fetchone()[0])
        ok = n == 0
        return DbCheckResult(
            "price_levels_active_stable_id",
            ok,
            f"active rows without stable_level_id: {n}",
            True,
        )
    finally:
        conn.close()


def check_ohlcv_spot_1d_binance(*, strict: bool) -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM ohlcv
            WHERE symbol = ? AND timeframe = '1d' AND ifnull(source,'') = 'binance'
            """,
            ("BTC/USDT",),
        )
        n_btc = int(cur.fetchone()[0])
        ok = n_btc >= MIN_OHLCV_1D_BARS_BINANCE
        detail = f"BTC/USDT 1d binance rows={n_btc}"

        if strict and ok:
            cur.execute(
                """
                SELECT MAX(timestamp) FROM ohlcv
                WHERE symbol = ? AND timeframe = '1d' AND ifnull(source,'') = 'binance'
                """,
                ("BTC/USDT",),
            )
            mx = cur.fetchone()[0]
            if mx is not None:
                age = int(time.time()) - int(mx)
                detail += f", last_ts_age_sec={age}"
                if age > STRICT_MAX_AGE_1D_BINANCE:
                    ok = False
                    detail += f" (strict max {STRICT_MAX_AGE_1D_BINANCE}s)"

        return DbCheckResult("ohlcv_btc_1d_binance", ok, detail, True)
    finally:
        conn.close()


def check_ohlcv_spot_coverage_1d(*, strict: bool) -> DbCheckResult:
    """Доля TRADING_SYMBOLS с хотя бы одной дневной свечой binance."""
    conn, cur = _cursor()
    try:
        if not TRADING_SYMBOLS:
            return DbCheckResult("ohlcv_spot_1d_coverage", True, "no TRADING_SYMBOLS", False)
        with_data = 0
        for sym in TRADING_SYMBOLS:
            cur.execute(
                """
                SELECT COUNT(*) FROM ohlcv
                WHERE symbol = ? AND timeframe = '1d' AND ifnull(source,'') = 'binance'
                """,
                (sym,),
            )
            if int(cur.fetchone()[0]) >= MIN_OHLCV_1D_BARS_BINANCE:
                with_data += 1
        ok = with_data >= min(MIN_OHLCV_1D_BARS_SAMPLE, len(TRADING_SYMBOLS))
        detail = f"symbols_with_1d_binance={with_data}/{len(TRADING_SYMBOLS)}"
        return DbCheckResult("ohlcv_spot_1d_coverage", ok, detail, not strict)
    finally:
        conn.close()


def check_ohlcv_btc_1m_freshness(*, strict: bool) -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute(
            """
            SELECT MAX(timestamp), COUNT(*) FROM ohlcv
            WHERE symbol = ? AND timeframe = '1m' AND ifnull(source,'') = 'binance'
            """,
            ("BTC/USDT",),
        )
        row = cur.fetchone()
        mx, cnt = row[0], int(row[1]) if row[1] is not None else 0
        if cnt == 0:
            return DbCheckResult(
                "ohlcv_btc_1m_binance",
                not strict,
                "no 1m rows for BTC/USDT binance",
                strict,
            )
        age = int(time.time()) - int(mx)
        detail = f"rows={cnt}, last_ts_age_sec={age}"
        ok = True
        if strict and age > STRICT_MAX_AGE_1M_BINANCE:
            ok = False
            detail += f" (strict max {STRICT_MAX_AGE_1M_BINANCE}s)"
        return DbCheckResult("ohlcv_btc_1m_binance", ok, detail, strict)
    finally:
        conn.close()


def check_instruments_atr() -> DbCheckResult:
    conn, cur = _cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM instruments
            WHERE exchange = 'bybit_futures' AND atr IS NOT NULL AND atr > 0
            """
        )
        n = int(cur.fetchone()[0])
        ok = n >= STRICT_MIN_INSTRUMENTS_WITH_ATR
        return DbCheckResult(
            "instruments_atr_bybit",
            ok,
            f"rows with atr>0: {n}",
            True,
        )
    finally:
        conn.close()


def run_db_integrity_checks(*, strict: bool = False) -> tuple[list[DbCheckResult], bool]:
    checks = [
        check_pragma_integrity(),
        check_schema_version(),
        check_active_levels_stable_id(),
        check_ohlcv_spot_1d_binance(strict=strict),
        check_ohlcv_spot_coverage_1d(strict=strict),
        check_ohlcv_btc_1m_freshness(strict=strict),
        check_instruments_atr(),
    ]
    failed_required = any(not r.ok and r.required for r in checks)
    return checks, not failed_required


__all__ = [
    "DbCheckResult",
    "MIN_SCHEMA_VERSION",
    "run_db_integrity_checks",
]
