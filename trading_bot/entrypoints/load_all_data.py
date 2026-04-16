"""
Полная синхронизация данных в SQLite: spot Binance, макро Yahoo, индексы TradingView,
Binance futures OI (история), Bybit OI/инструменты.

Ликвидации Bybit по умолчанию отключены (WS может висеть/долго реконнектиться).
Включаются только явным флагом `--bybit-ws-liquidations`.

Пути: SQLite — `trading_bot/data/market_data.db`; `.env` — см. `trading_bot/config/settings.py`
(`trading_bot/.env`, затем корень репо `REPO_ROOT/.env`).

Символы: спот под торговлю — `TRADING_SYMBOLS`; доп. спот контекста — `ANALYTIC_SYMBOLS['crypto_context']`
(без дубля запросов для пар уже в TRADING); макро/индексы — остальные ключи `ANALYTIC_SYMBOLS`.

  python trading_bot/entrypoints/load_all_data.py       # из корня репо (рядом с config.py)
  python trading_bot/entrypoints/load_all_data.py --full
  python trading_bot/entrypoints/load_all_data.py --bybit-ws-liquidations

  Шаг «Binance futures open interest (history)» можно отключить: LOAD_ALL_SKIP_BINANCE_FUTURES_OI=1 в .env.

  Те же переключатели, что у supervisor DATA_REFRESH (см. defaults в settings.py).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from trading_bot.config import settings as st
from trading_bot.config.symbols import (
    ANALYTIC_SYMBOLS,
    TRADING_SYMBOLS,
    crypto_context_binance_spot_not_in_trading,
)
from trading_bot.data.collectors import update_all_futures_data, update_indices
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.schema import init_db, run_migrations

logger = logging.getLogger(__name__)


def _safe(name: str, fn) -> None:
    try:
        fn()
    except Exception:
        logger.exception("%s failed", name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        help="Полная перезагрузка spot/macro/indices с HISTORY_START (очень долго)",
    )
    parser.add_argument(
        "--bybit-ws-liquidations",
        action="store_true",
        help="Включить сбор ликвидаций Bybit через WebSocket (может сильно замедлить запуск)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    init_db()
    run_migrations()
    mgr = DataLoaderManager()

    symbols_spot = list(TRADING_SYMBOLS)
    spot_ctx_extra = crypto_context_binance_spot_not_in_trading()
    macro_syms = list(ANALYTIC_SYMBOLS.get("macro", []))
    indices_syms = list(ANALYTIC_SYMBOLS.get("indices", []))
    logger.info(
        "Конфиг: TRADING_SYMBOLS=%d шт.; crypto_context extra spot (Binance)=%s; macro=%s; indices=%s",
        len(symbols_spot),
        spot_ctx_extra,
        macro_syms,
        indices_syms,
    )

    if args.full:
        if st.SUPERVISOR_DATA_REFRESH_SPOT_MAIN:
            logger.info("=== Full historical: spot (4h,1d,1w,1M) — все TRADING_SYMBOLS ===")
            mgr.load_historical_spot(symbols=symbols_spot, force_full=True)
            logger.info("=== Spot 1m window — все TRADING_SYMBOLS ===")
            mgr.load_intraday_1m_spot(symbols=symbols_spot, force_full=True)
        else:
            logger.info("Skipped full spot TRADING_SYMBOLS (SUPERVISOR_DATA_REFRESH_SPOT_MAIN=0)")
        if spot_ctx_extra and st.SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT:
            logger.info(
                "=== Full historical spot — ANALYTIC_SYMBOLS['crypto_context'] "
                "(только пары вне TRADING_SYMBOLS) ==="
            )
            mgr.load_historical_spot(symbols=spot_ctx_extra, force_full=True)
            logger.info("=== Spot 1m window — crypto_context extra ===")
            mgr.load_intraday_1m_spot(symbols=spot_ctx_extra, force_full=True)
        elif spot_ctx_extra:
            logger.info("Skipped full crypto_context spot (SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT=0)")
        if st.SUPERVISOR_DATA_REFRESH_MACRO:
            logger.info("=== Macro — ANALYTIC_SYMBOLS['macro'] ===")
            mgr.load_historical_macro(symbols=macro_syms, force_full=True)
        else:
            logger.info("Skipped macro (SUPERVISOR_DATA_REFRESH_MACRO=0)")
        if st.SUPERVISOR_DATA_REFRESH_INDICES_TV:
            logger.info("=== TradingView indices — ANALYTIC_SYMBOLS['indices'] ===")
            mgr.load_historical_tradingview_indices(symbols=indices_syms, force_full=True)
        else:
            logger.info("Skipped TradingView indices (SUPERVISOR_DATA_REFRESH_INDICES_TV=0)")
    else:
        if st.SUPERVISOR_DATA_REFRESH_SPOT_MAIN:
            logger.info("=== Incremental spot — все TRADING_SYMBOLS ===")
            mgr.update_incremental_spot(symbols=symbols_spot)
        else:
            logger.info("Skipped incremental spot TRADING_SYMBOLS (SUPERVISOR_DATA_REFRESH_SPOT_MAIN=0)")
        if spot_ctx_extra and st.SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT:
            logger.info("=== Incremental spot — ANALYTIC_SYMBOLS['crypto_context'] (extra) ===")
            mgr.update_incremental_spot(symbols=spot_ctx_extra)
        elif spot_ctx_extra:
            logger.info("Skipped incremental crypto_context spot (SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT=0)")
        if st.SUPERVISOR_DATA_REFRESH_MACRO:
            logger.info("=== Incremental macro — ANALYTIC_SYMBOLS['macro'] ===")
            mgr.update_incremental_macro(symbols=macro_syms)
        else:
            logger.info("Skipped incremental macro (SUPERVISOR_DATA_REFRESH_MACRO=0)")
        if st.SUPERVISOR_DATA_REFRESH_INDICES_TV:
            logger.info("=== TradingView indices (collector) ===")
            update_indices()
        else:
            logger.info("Skipped TradingView indices (SUPERVISOR_DATA_REFRESH_INDICES_TV=0)")

    _skip_binance_futures_oi = (os.getenv("LOAD_ALL_SKIP_BINANCE_FUTURES_OI") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if _skip_binance_futures_oi:
        logger.info(
            "Skipped Binance futures open interest (LOAD_ALL_SKIP_BINANCE_FUTURES_OI=1; см. collectors.update_all_futures_data)"
        )
    else:
        logger.info("=== Binance futures open interest (history) ===")
        _safe("update_all_futures_data", lambda: update_all_futures_data(days_back=90))

    if st.SUPERVISOR_DATA_REFRESH_OI_BYBIT:
        logger.info("=== Open interest incremental (Bybit) — все TRADING_SYMBOLS ===")
        _safe("update_incremental_oi", lambda: mgr.update_incremental_oi(symbols=symbols_spot))
    else:
        logger.info("Skipped Bybit OI incremental (SUPERVISOR_DATA_REFRESH_OI_BYBIT=0)")

    if args.bybit_ws_liquidations:
        logger.info("=== Liquidations (Bybit WS) — все TRADING_SYMBOLS ===")
        _safe("update_liquidations", lambda: mgr.update_liquidations(symbols=symbols_spot))
    else:
        logger.info(
            "Skipped Bybit liquidations WS (use --bybit-ws-liquidations to enable)"
        )

    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS:
        logger.info("=== Instruments (Bybit) — все TRADING_SYMBOLS ===")
        _safe(
            "update_instruments_for_symbols",
            lambda: mgr.update_instruments_for_symbols(symbols_spot),
        )
    else:
        logger.info("Skipped Bybit instruments (SUPERVISOR_DATA_REFRESH_INSTRUMENTS=0)")

    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR:
        logger.info("=== ATR (instruments, Gerchik from spot 1d in DB) ===")
        _safe(
            "update_instruments_atr_for_trading_symbols",
            lambda: mgr.update_instruments_atr_for_trading_symbols(),
        )
    else:
        logger.info("Skipped instruments ATR (SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR=0)")

    logger.info("=== DB integrity (non-strict) ===")
    try:
        from trading_bot.data.db_integrity import run_db_integrity_checks

        results, ok = run_db_integrity_checks(strict=False)
        for r in results:
            if not r.ok:
                lvl = logging.ERROR if r.required else logging.WARNING
                logger.log(lvl, "%s: %s", r.name, r.detail)
            else:
                logger.info("%s: %s", r.name, r.detail)
        if not ok:
            logger.error(
                "DB integrity: required checks failed — проверь DB_PATH и миграции, "
                "затем: python -m trading_bot.scripts.check_db_integrity"
            )
        else:
            logger.info(
                "Сводка окружения и БД (пути, счётчики, проверки): "
                "python -m trading_bot.scripts.data_foundation_status"
            )
    except Exception:
        logger.exception("DB integrity check failed to run")

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
