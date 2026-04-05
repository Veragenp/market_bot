import time

import pytest

from trading_bot.data.bybit_futures_loader import BybitFuturesDataLoader
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.repositories import MetadataRepository


def test_fetch_open_interest_pagination_and_change(monkeypatch, clean_db):
    loader = BybitFuturesDataLoader()

    ts1 = 1_700_000_000  # requested start
    ts2 = ts1 + 3600  # requested end
    ts_prev = ts1 - 86400
    ts2_prev = ts2 - 86400

    # Mock pagination: first page has ts2 + ts1, second page has ts_prev.
    def fake_get_json(path, params):
        assert path == "/v5/market/open-interest"
        cursor = params.get("cursor")
        if not cursor:
            return {
                "result": {
                    "list": [
                        {"openInterest": "130", "timestamp": str(ts2 * 1000)},
                        {"openInterest": "100", "timestamp": str(ts1 * 1000)},
                    ],
                    "nextPageCursor": "c1",
                }
            }
        if cursor == "c1":
            return {
                "result": {
                    "list": [
                        {"openInterest": "50", "timestamp": str(ts_prev * 1000)},
                        {"openInterest": "100", "timestamp": str(ts2_prev * 1000)},
                    ],
                    "nextPageCursor": "",
                }
            }
        raise AssertionError(f"Unexpected cursor: {cursor}")

    monkeypatch.setattr(loader, "_get_json", fake_get_json)

    records = loader.fetch_open_interest(symbol="BTC/USDT", interval="1h", start_ts=ts1, end_ts=ts2)
    assert [r["timestamp"] for r in records] == [ts1, ts2]

    by_ts = {r["timestamp"]: r for r in records}
    assert by_ts[ts1]["oi_value"] == 100.0
    assert by_ts[ts1]["oi_change_24h"] == 50.0  # 100 - 50

    assert by_ts[ts2]["oi_value"] == 130.0
    assert by_ts[ts2]["oi_change_24h"] == 30.0  # 130 - 100

    assert all(r["exchange"] == loader.get_exchange_name() for r in records)


def test_liquidation_item_to_event_parses_fields():
    loader = BybitFuturesDataLoader()
    item = {"T": "2000000000000", "S": "Buy", "v": "2.5", "p": "10"}
    ev = loader._liquidation_item_to_event(item)  # type: ignore[attr-defined]
    assert ev is not None
    assert ev["timestamp"] == 2_000_000_000
    assert ev["side"] == "Buy"
    assert ev["qty"] == 2.5
    assert ev["price"] == 10.0


def test_update_liquidations_aggregates_and_saves(monkeypatch, clean_db):
    manager = DataLoaderManager()

    ts_base = 1_700_000_000
    bucket1 = (ts_base // 3600) * 3600
    bucket2 = bucket1 + 3600

    events = [
        {"timestamp": bucket1 + 10, "side": "Buy", "qty": 2, "price": 5},   # short += 10
        {"timestamp": bucket1 + 20, "side": "Sell", "qty": 3, "price": 4},  # long  += 12
        {"timestamp": bucket2 + 100, "side": "Sell", "qty": 1, "price": 10},  # long += 10
    ]

    monkeypatch.setattr(manager.bybit_loader, "fetch_liquidations", lambda symbol, start_ts=None, end_ts=None: events)

    manager.update_liquidations(symbols=["BTC/USDT"], aggregate_timeframes=["1h"])

    # Verify saved buckets.
    from trading_bot.data.db import get_connection

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timeframe, timestamp, long_volume, short_volume, total_volume
        FROM liquidations
        WHERE symbol = ? AND timeframe = ?
        ORDER BY timestamp
        """,
        ("BTC/USDT", "1h"),
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 2

    by_ts = {int(r["timestamp"]): r for r in rows}
    assert float(by_ts[bucket1]["short_volume"]) == 10.0
    assert float(by_ts[bucket1]["long_volume"]) == 12.0
    assert float(by_ts[bucket1]["total_volume"]) == 22.0

    assert float(by_ts[bucket2]["short_volume"]) == 0.0
    assert float(by_ts[bucket2]["long_volume"]) == 10.0
    assert float(by_ts[bucket2]["total_volume"]) == 10.0

    # Verify metadata cursor was updated.
    meta = MetadataRepository()
    source = manager.bybit_loader.get_exchange_name()
    last_updated = meta.get_last_updated("liquidations:BTC/USDT", "1h", source)
    assert int(last_updated) == bucket2


def test_update_incremental_oi_uses_metadata_cursor(monkeypatch, clean_db):
    manager = DataLoaderManager()
    source = manager.bybit_loader.get_exchange_name()

    ts_last = 1_700_000_000
    timeframe = "1h"
    meta_symbol = "open_interest:BTC/USDT"
    manager.meta_repo.update(
        meta_symbol,
        timeframe,
        source=source,
        last_updated=ts_last,
        last_full_update=int(time.time()),
    )

    seen = {}

    def fake_fetch_open_interest(symbol, interval, start_ts=None, end_ts=None):
        seen["symbol"] = symbol
        seen["interval"] = interval
        seen["start_ts"] = start_ts
        seen["end_ts"] = end_ts
        # Return a single record after cursor.
        return [
            {"timestamp": ts_last + 1, "exchange": source, "oi_value": 10.0, "oi_change_24h": None},
        ]

    monkeypatch.setattr(manager.bybit_loader, "fetch_open_interest", fake_fetch_open_interest)

    manager.update_incremental_oi(symbols=["BTC/USDT"], timeframes=[timeframe])
    assert seen["start_ts"] == ts_last + 1

    from trading_bot.data.db import get_connection

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(timestamp) FROM open_interest
        WHERE symbol = ? AND timeframe = ? AND source = ?
        """,
        ("BTC/USDT", timeframe, source),
    )
    max_ts = cur.fetchone()[0]
    conn.close()

    assert int(max_ts) == ts_last + 1

