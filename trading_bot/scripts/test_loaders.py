"""Smoke test loaders before full historical loads.

Checks:
  - data is non-empty
  - timestamps are ordered
  - `source` is filled
  - metadata is updated
  - liquidity filter works (is_tradable)

Usage:
  python -m trading_bot.scripts.test_loaders
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

import pandas as pd

from trading_bot.config.settings import LIQUIDATIONS_MAX_RECORDS, MIN_AVG_VOLUME_24H
from trading_bot.data.db import DB_PATH, get_connection
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.repositories import InstrumentsRepository
from trading_bot.data.schema import init_db, run_migrations

logger = logging.getLogger(__name__)


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _tail20_by_ts(records: List[Dict[str, Any]], *, limit: int = 20) -> List[Dict[str, Any]]:
    if not records:
        return []
    records_sorted = sorted(records, key=lambda r: int(r["timestamp"]))
    return records_sorted[-limit:]


def _assert_ordered(records: Sequence[Dict[str, Any]]) -> None:
    if not records:
        return
    ts = [int(r["timestamp"]) for r in records]
    assert ts == sorted(ts), "timestamps are not ordered ascending"


def _approx_start_ts(end_ts: int, timeframe: str, *, bars: int) -> int:
    # Approximations for selecting a small window for smoke-test.
    tf_seconds = {
        "1m": 60,
        "4h": 4 * 3600,
        "1h": 3600,
        "1d": 86400,
        "1w": 7 * 86400,
        "1M": 30 * 86400,  # approx month
    }.get(timeframe)

    if tf_seconds is None:
        raise ValueError(f"Unsupported timeframe for smoke window: {timeframe!r}")

    return int(end_ts - bars * tf_seconds)


def _save_ohlcv_with_metadata(
    manager: DataLoaderManager,
    *,
    symbol: str,
    timeframe: str,
    records: List[Dict[str, Any]],
    source: str,
) -> None:
    if not records:
        return

    manager.ohlcv_repo.save_batch(symbol, timeframe, records)
    max_ts = max(int(r["timestamp"]) for r in records)
    now_ts = int(time.time())

    existing = manager.meta_repo.get_last_updated(symbol, timeframe, source)
    if existing is None:
        manager.meta_repo.update(
            symbol,
            timeframe,
            source=source,
            last_updated=max_ts,
            last_full_update=now_ts,
        )
    else:
        manager.meta_repo.update(symbol, timeframe, source=source, last_updated=max_ts)


def _check_ohlcv(
    *,
    name: str,
    manager: DataLoaderManager,
    loader: Any,
    symbol: str,
    timeframes: Sequence[str],
    bars: int = 20,
) -> None:
    source = loader.get_exchange_name()
    end_ts = int(time.time())

    print(f"\n== {name} ==")
    print(f"Symbol: {symbol}")
    print(f"Expected Source: {source}")

    for tf in timeframes:
        try:
            start_ts = _approx_start_ts(end_ts, tf, bars=bars)
            records = loader.fetch_ohlcv(symbol=symbol, timeframe=tf, start_ts=start_ts, end_ts=end_ts)
            records = _tail20_by_ts(records, limit=bars)

            if not records:
                print(f"[{tf}] EMPTY")
                continue

            _assert_ordered(records)
            rec0 = records[0]
            assert rec0.get("source") == source, f"bad source: {rec0.get('source')!r} != {source!r}"

            print(f"\n[{tf}] rows={len(records)}")
            df = pd.DataFrame(records)
            df["timestamp_utc"] = df["timestamp"].map(_fmt_ts)
            show_cols = [c for c in ["timestamp_utc", "open", "high", "low", "close", "volume", "source"] if c in df.columns]
            print(df[show_cols].tail(5).to_string(index=False))

            _save_ohlcv_with_metadata(manager, symbol=symbol, timeframe=tf, records=records, source=source)
            meta_last = manager.meta_repo.get_last_updated(symbol, tf, source)
            print(f"Meta last_updated: {_fmt_ts(int(meta_last)) if meta_last else None}")
        except Exception as exc:
            logger.exception("Failed check %s %s %s: %s", name, symbol, tf, exc)
            print(f"[{tf}] ERROR: {exc}")


def _ensure_instruments_for_is_tradable(symbol_bybit: str, avg_volume_24h: float, *, exchange: str = "bybit_futures") -> None:
    repo = InstrumentsRepository()
    repo.save_or_update(symbol=symbol_bybit, exchange=exchange, data={"avg_volume_24h": float(avg_volume_24h)})


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    # Optional: reset DB for clean smoke-testing
    if os.getenv("SMOKE_RESET_DB", "0") == "1" and os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    init_db()
    run_migrations()

    manager = DataLoaderManager()

    # 1) Spot (Binance)
    _check_ohlcv(
        name="Spot (Binance)",
        manager=manager,
        loader=manager.spot_loader,
        symbol="BTC/USDT",
        timeframes=["1m", "4h", "1d", "1w", "1M"],
        bars=20,
    )

    # 2) Macro (Yahoo)
    _check_ohlcv(
        name="Macro (Yahoo)",
        manager=manager,
        loader=manager.macro_loader,
        symbol="SP500",
        timeframes=["4h", "1d", "1w", "1M"],
        bars=20,
    )

    # 3) Indices (TradingView) - loader in this repo supports only 1d/1w/1M
    _check_ohlcv(
        name="Indices (TradingView)",
        manager=manager,
        loader=manager.tv_loader,
        symbol="TOTAL",
        timeframes=["1d", "1w", "1M"],
        bars=20,
    )

    # 4) Futures (Bybit) - instrument info + OI + liquidations
    print("\n== Futures (Bybit) ==")
    bybit_symbol_internal = "BTC/USDT"

    try:
        inst = manager.bybit_loader.fetch_instrument_info(bybit_symbol_internal)
        print("Instrument info (BTC/USDT):")
        for k, v in inst.items():
            print(f"  {k}: {v}")
    except Exception as exc:
        print(f"Instrument info ERROR: {exc}")

    # OI smoke: last ~20 1h points
    try:
        source = manager.bybit_loader.get_exchange_name()
        now_ts = int(time.time())
        timeframe = "1h"
        start_ts = _approx_start_ts(now_ts, timeframe, bars=20)
        oi_records = manager.bybit_loader.fetch_open_interest(
            symbol=bybit_symbol_internal,
            interval=timeframe,
            start_ts=start_ts,
            end_ts=now_ts,
        )
        oi_records = _tail20_by_ts(oi_records, limit=20)

        if not oi_records:
            print("OI: EMPTY")
        else:
            _assert_ordered(oi_records)
            meta_symbol = f"open_interest:{bybit_symbol_internal}"
            last_existing = manager.meta_repo.get_last_updated(meta_symbol, timeframe, source)
            if last_existing is not None:
                oi_records = [r for r in oi_records if int(r["timestamp"]) > int(last_existing)]

            print(f"OI: rows={len(oi_records)} Source={source}")
            df = pd.DataFrame(oi_records)
            if not df.empty:
                df["timestamp_utc"] = df["timestamp"].map(_fmt_ts)
                print(df[["timestamp_utc", "oi_value", "oi_change_24h"]].tail(5).to_string(index=False))

            if oi_records:
                manager.oi_repo.save_batch(bybit_symbol_internal, timeframe, records=oi_records, source=source)
                max_ts = max(int(r["timestamp"]) for r in oi_records)
                if last_existing is None:
                    manager.meta_repo.update(
                        meta_symbol,
                        timeframe,
                        source=source,
                        last_updated=max_ts,
                        last_full_update=int(time.time()),
                    )
                else:
                    manager.meta_repo.update(meta_symbol, timeframe, source=source, last_updated=max_ts)
                meta_last = manager.meta_repo.get_last_updated(meta_symbol, timeframe, source)
                print(f"OI meta last_updated: {_fmt_ts(int(meta_last)) if meta_last else None}")
    except Exception as exc:
        logger.exception("OI smoke error: %s", exc)
        print(f"OI ERROR: {exc}")

    # Liquidations smoke: update buckets with current websocket feed
    try:
        manager.update_liquidations(symbols=[bybit_symbol_internal], aggregate_timeframes=["1h"])
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timeframe, timestamp, long_volume, short_volume, total_volume, source
            FROM liquidations
            WHERE symbol = ? AND timeframe = '1h'
            ORDER BY timestamp DESC
            LIMIT 10
            """,
            (bybit_symbol_internal,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        print(f"Liquidations (1h) last rows: {len(rows)} (collected cap ~{LIQUIDATIONS_MAX_RECORDS})")
        if rows:
            df = pd.DataFrame(rows)
            df["timestamp_utc"] = df["timestamp"].map(_fmt_ts)
            df = df.sort_values("timestamp")
            print(df[["timestamp_utc", "long_volume", "short_volume", "total_volume", "source"]].tail(5).to_string(index=False))
    except Exception as exc:
        logger.exception("Liquidations smoke error: %s", exc)
        print(f"Liquidations ERROR: {exc}")

    # 5) Liquidity filtering smoke for is_tradable()
    print("\n== Liquidity filtering ==")
    try:
        # Make one symbol clearly above threshold, another clearly below.
        _ensure_instruments_for_is_tradable("BTCUSDT", float(MIN_AVG_VOLUME_24H) + 1.0)
        _ensure_instruments_for_is_tradable("ETHUSDT", 1.0)

        ok = manager.is_tradable("BTC/USDT")
        bad = manager.is_tradable("ETH/USDT")
        print(f"is_tradable(BTC/USDT) -> {ok} (expected True)")
        print(f"is_tradable(ETH/USDT) -> {bad} (expected False)")
    except Exception as exc:
        logger.exception("Liquidity filtering error: %s", exc)
        print(f"Liquidity filtering ERROR: {exc}")


if __name__ == "__main__":
    main()
