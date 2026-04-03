import os

import pytest

from trading_bot.config.settings import MIN_AVG_VOLUME_24H
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.db import DB_PATH
from trading_bot.data.repositories import InstrumentsRepository, save_ohlcv
from trading_bot.data.schema import init_db, run_migrations


@pytest.fixture
def clean_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()
    run_migrations()
    yield
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def test_is_tradable_filters_by_avg_volume(clean_db):
    repo = InstrumentsRepository()
    manager = DataLoaderManager()

    repo.save_or_update(
        symbol="BTCUSDT",
        exchange="bybit_futures",
        data={"avg_volume_24h": float(MIN_AVG_VOLUME_24H) + 1},
    )
    repo.save_or_update(
        symbol="ETHUSDT",
        exchange="bybit_futures",
        data={"avg_volume_24h": 1.0},
    )

    assert manager.is_tradable("BTC/USDT") is True
    assert manager.is_tradable("ETH/USDT") is False
    assert manager.is_tradable("UNKNOWN/USDT") is False


def test_update_instruments_for_symbols_saves_to_db(clean_db, monkeypatch):
    manager = DataLoaderManager()

    def fake_fetch_all_instruments_info(symbols_to_filter=None, *, page_limit=500):
        return [
            {
                "symbol": "BTCUSDT",
                "exchange": "bybit_futures",
                "tick_size": 0.01,
                "min_qty": 0.001,
                "avg_volume_24h": float(MIN_AVG_VOLUME_24H) + 123,
                "commission_open": None,
                "commission_close": None,
            }
        ]

    monkeypatch.setattr(manager.bybit_loader, "fetch_all_instruments_info", fake_fetch_all_instruments_info)

    updated = manager.update_instruments_for_symbols(["BTC/USDT"])
    assert updated == 1

    repo = InstrumentsRepository()
    rec = repo.get("BTCUSDT", "bybit_futures")
    assert rec is not None
    assert float(rec["avg_volume_24h"]) >= float(MIN_AVG_VOLUME_24H)


def test_get_liquid_symbols_from_instruments_returns_internal_format(clean_db):
    repo = InstrumentsRepository()
    manager = DataLoaderManager()

    repo.save_or_update(
        symbol="BTCUSDT",
        exchange="bybit_futures",
        data={"avg_volume_24h": float(MIN_AVG_VOLUME_24H) + 10},
    )
    repo.save_or_update(
        symbol="ETHUSDT",
        exchange="bybit_futures",
        data={"avg_volume_24h": 10.0},
    )

    liquid = manager.get_liquid_symbols_from_instruments()
    assert "BTC/USDT" in liquid
    assert "ETH/USDT" not in liquid


def test_instruments_atr_update_from_ohlcv(clean_db):
    repo = InstrumentsRepository()
    repo.save_or_update(
        symbol="BTCUSDT",
        exchange="bybit_futures",
        data={
            "tick_size": 0.1,
            "min_qty": 0.001,
            "avg_volume_24h": float(MIN_AVG_VOLUME_24H) + 1.0,
        },
    )
    base_ts = 1_700_000_000
    day = 86400
    recs = []
    for i in range(20):
        recs.append(
            {
                "timestamp": base_ts + i * day,
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1.0,
                "source": "binance",
            }
        )
    save_ohlcv("BTC/USDT", "1d", recs)

    manager = DataLoaderManager()
    n = manager.update_instruments_atr_for_trading_symbols()
    assert n == 1
    rec = repo.get("BTCUSDT", "bybit_futures")
    assert rec is not None
    assert rec.get("atr") is not None
    assert float(rec["atr"]) > 0


def test_save_instrument_from_bybit_preserves_atr(clean_db):
    repo = InstrumentsRepository()
    repo.save_or_update(
        symbol="ETHUSDT",
        exchange="bybit_futures",
        data={"tick_size": 0.01, "min_qty": 0.01, "avg_volume_24h": 1e9},
    )
    repo.update_atr("ETHUSDT", "bybit_futures", 2.5)
    repo.save_or_update(
        symbol="ETHUSDT",
        exchange="bybit_futures",
        data={
            "tick_size": 0.02,
            "min_qty": 0.02,
            "avg_volume_24h": 2e9,
            "commission_open": 0.0005,
            "commission_close": 0.0002,
        },
    )
    rec = repo.get("ETHUSDT", "bybit_futures")
    assert rec is not None
    assert float(rec["atr"]) == 2.5
    assert float(rec["tick_size"]) == 0.02

