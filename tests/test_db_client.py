import time
import pytest
from trading_bot.data.db_client import (
    save_ohlcv, get_ohlcv, get_last_update, update_metadata,
    save_liquidations, save_open_interest, clean_old_minute_data
)
from trading_bot.data.db import DB_PATH
import os

@pytest.fixture
def clean_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    from trading_bot.data.schema import init_db, run_migrations
    init_db()
    run_migrations()
    yield
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_save_and_get_ohlcv(clean_db):
    symbol = "BTC/USDT"
    timeframe = "1d"
    now = int(time.time())
    records = [{
        "timestamp": now,
        "open": 50000,
        "high": 51000,
        "low": 49000,
        "close": 50500,
        "volume": 1000,
        "source": "binance"
    }]
    save_ohlcv(symbol, timeframe, records)
    data = get_ohlcv(symbol, timeframe, start=now-86400, end=now+86400)
    assert len(data) == 1
    assert data[0]["close"] == 50500

def test_metadata(clean_db):
    symbol = "ETH/USDT"
    timeframe = "1h"
    last = 1234567890
    update_metadata(symbol, timeframe, last)
    assert get_last_update(symbol, timeframe) == last
    # Обновляем
    new_last = 1234567899
    update_metadata(symbol, timeframe, new_last)
    assert get_last_update(symbol, timeframe) == new_last

def test_clean_old_minute_data(clean_db):
    symbol = "TEST"
    timeframe = "1m"
    old_ts = int(time.time()) - 31*86400  # 31 день назад
    recent_ts = int(time.time()) - 1*86400  # 1 день назад
    records = [
        {"timestamp": old_ts, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        {"timestamp": recent_ts, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2}
    ]
    save_ohlcv(symbol, timeframe, records)
    clean_old_minute_data(30)  # удалить старше 30 дней
    data = get_ohlcv(symbol, timeframe)
    assert len(data) == 1
    assert data[0]["close"] == 2