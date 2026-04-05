"""
Проверка наполнения SQLite (свежесть данных).

Запуск из корня репозитория (рядом с config.py):
  python trading_bot/entrypoints/healthcheck_data.py
  python trading_bot/entrypoints/healthcheck_data.py --strict

Обязательные проверки (strict): spot Binance 1m/1h/4h, Bybit OI 4h, индекс TOTAL 4h (TradingView).
Макро SP500 1d — мягкая (предупреждение), если ещё не было суточного job.
Ликвидации Bybit — информационно (поток может долго не давать событий).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    required: bool


def _age_sec(max_ts: Optional[int]) -> Optional[int]:
    if max_ts is None:
        return None
    return int(time.time()) - int(max_ts)


def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ts)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Проверка свежести данных в market_data.db")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Код выхода 1, если не прошла хотя бы одна обязательная проверка",
    )
    args = parser.parse_args()

    from trading_bot.data.db import get_connection
    from trading_bot.data.schema import init_db, run_migrations
    from trading_bot.config.settings import DB_PATH

    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()

    def qmax(sql: str, params: tuple = ()) -> Optional[int]:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    now = int(time.time())
    results: list[CheckResult] = []

    # --- Spot Binance (основной контур scheduler) ---
    t1m = qmax(
        """
        SELECT MAX(timestamp) FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m' AND ifnull(source,'') = 'binance'
        """,
        ("BTC/USDT",),
    )
    age_1m = _age_sec(t1m)
    ok_1m = age_1m is not None and age_1m <= 3600
    results.append(
        CheckResult(
            "spot BTC/USDT 1m (binance)",
            ok_1m,
            f"last={_fmt_ts(t1m)}, age={age_1m}s (порог 3600s; планировщик 1m раз в 15 мин)",
            True,
        )
    )

    t1h = qmax(
        """
        SELECT MAX(timestamp) FROM ohlcv
        WHERE symbol = ? AND timeframe = '1h' AND ifnull(source,'') = 'binance'
        """,
        ("BTC/USDT",),
    )
    age_1h = _age_sec(t1h)
    ok_1h = age_1h is not None and age_1h <= 7200
    results.append(
        CheckResult(
            "spot BTC/USDT 1h (binance)",
            ok_1h,
            f"last={_fmt_ts(t1h)}, age={age_1h}s (порог 7200s)",
            True,
        )
    )

    t4h = qmax(
        """
        SELECT MAX(timestamp) FROM ohlcv
        WHERE symbol = ? AND timeframe = '4h' AND ifnull(source,'') = 'binance'
        """,
        ("BTC/USDT",),
    )
    age_4h = _age_sec(t4h)
    ok_4h = age_4h is not None and age_4h <= 6 * 3600
    results.append(
        CheckResult(
            "spot BTC/USDT 4h (binance)",
            ok_4h,
            f"last={_fmt_ts(t4h)}, age={age_4h}s (порог 6h)",
            True,
        )
    )

    # --- Bybit open interest ---
    oi4 = qmax(
        """
        SELECT MAX(timestamp) FROM open_interest
        WHERE symbol = ? AND timeframe = '4h' AND source = 'bybit_futures'
        """,
        ("BTC/USDT",),
    )
    age_oi = _age_sec(oi4)
    ok_oi = age_oi is not None and age_oi <= 8 * 3600
    results.append(
        CheckResult(
            "OI BTC/USDT 4h (bybit_futures)",
            ok_oi,
            f"last={_fmt_ts(oi4)}, age={age_oi}s (порог 8h; без backfill может быть пусто)",
            True,
        )
    )

    # --- TradingView indices ---
    tv = qmax(
        """
        SELECT MAX(timestamp) FROM ohlcv
        WHERE symbol = 'TOTAL' AND timeframe = '4h' AND ifnull(source,'') = 'tradingview'
        """
    )
    age_tv = _age_sec(tv)
    ok_tv = age_tv is not None and age_tv <= 6 * 3600
    results.append(
        CheckResult(
            "index TOTAL 4h (tradingview)",
            ok_tv,
            f"last={_fmt_ts(tv)}, age={age_tv}s (порог 6h)",
            True,
        )
    )

    # --- Macro yfinance (после первого суточного job) ---
    sp = qmax(
        """
        SELECT MAX(timestamp) FROM ohlcv
        WHERE symbol = 'SP500' AND timeframe = '1d' AND ifnull(source,'') = 'yfinance'
        """
    )
    age_sp = _age_sec(sp)
    ok_sp = age_sp is not None and age_sp <= 3 * 86400
    results.append(
        CheckResult(
            "macro SP500 1d (yfinance)",
            ok_sp,
            f"last={_fmt_ts(sp)}, age={age_sp}s (порог 3d; job в 02:30 UTC)",
            False,
        )
    )

    # --- Liquidations Bybit (может быть пусто долго) ---
    cur.execute(
        "SELECT COUNT(1) FROM liquidations WHERE source = 'bybit_futures'",
    )
    n_liq_row = cur.fetchone()
    n_liq = int(n_liq_row[0]) if n_liq_row and n_liq_row[0] is not None else 0
    liq_last = qmax(
        """
        SELECT MAX(timestamp) FROM liquidations
        WHERE source = 'bybit_futures' AND symbol = 'BTC/USDT'
        """
    )
    results.append(
        CheckResult(
            "liquidations bybit_futures (любые)",
            True,
            f"rows={n_liq or 0}, BTC last bucket={_fmt_ts(liq_last)} (WS — события редки)",
            False,
        )
    )

    conn.close()

    print(f"DB: {DB_PATH}")
    print(f"now UTC: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(now))}")
    print()

    failed_required = False
    for r in results:
        mark = "OK " if r.ok else "FAIL"
        req = "[обязательно]" if r.required else "[инфо]   "
        print(f"{mark} {req} {r.name}")
        print(f"     {r.detail}")
        if r.required and not r.ok:
            failed_required = True

    print()
    if failed_required:
        print("Итог: обязательные проверки не пройдены.")
        print(
            "Подсказка: один раз залейте историю (spot / OI / индексы), "
            "затем запустите trading_bot/entrypoints/run_scheduler.py."
        )
        return 1 if args.strict else 0

    print("Итог: обязательные проверки пройдены.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
