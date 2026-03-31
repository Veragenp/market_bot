import pandas as pd
import pytest

from trading_bot.provider.clients.yfinance import YFinanceClient


def _sample_df(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [1.0] * len(index),
            "High": [2.0] * len(index),
            "Low": [0.5] * len(index),
            "Close": [1.5] * len(index),
            "Volume": [100.0] * len(index),
        },
        index=index,
    )


def test_fetch_ohlcv_rejects_unknown_symbol():
    client = YFinanceClient({"SP500": "^GSPC"})
    with pytest.raises(ValueError):
        client.fetch_ohlcv("UNKNOWN", "1d")


def test_fetch_ohlcv_rejects_unsupported_timeframe():
    client = YFinanceClient({"SP500": "^GSPC"})
    with pytest.raises(ValueError):
        client.fetch_ohlcv("SP500", "4h")


def test_fetch_ohlcv_aligns_daily_to_utc_midnight(monkeypatch):
    client = YFinanceClient({"SP500": "^GSPC"})
    idx = pd.date_range("2025-01-06 16:00:00", periods=2, freq="D")
    df = _sample_df(idx)
    monkeypatch.setattr(client, "_download_data", lambda **kwargs: df)

    rows = client.fetch_ohlcv("SP500", "1d")
    assert len(rows) == 2
    assert rows[0]["timestamp"] % 86400 == 0
    assert rows[1]["timestamp"] % 86400 == 0
    assert rows[0]["source"] == "yfinance"


def test_fetch_ohlcv_aligns_weekly_to_monday_utc(monkeypatch):
    client = YFinanceClient({"SP500": "^GSPC"})
    # Wednesday and next Friday in exchange-local naive time.
    idx = pd.to_datetime(["2025-01-08 16:00:00", "2025-01-17 16:00:00"])
    df = _sample_df(pd.DatetimeIndex(idx))
    monkeypatch.setattr(client, "_download_data", lambda **kwargs: df)

    rows = client.fetch_ohlcv("SP500", "1w")
    assert len(rows) == 2
    mondays = [pd.Timestamp(r["timestamp"], unit="s", tz="UTC").weekday() for r in rows]
    assert mondays == [0, 0]
    assert all(pd.Timestamp(r["timestamp"], unit="s", tz="UTC").hour == 0 for r in rows)


def test_fetch_ohlcv_aligns_monthly_to_first_day(monkeypatch):
    client = YFinanceClient({"SP500": "^GSPC"})
    idx = pd.to_datetime(["2025-01-20 16:00:00", "2025-02-18 16:00:00"])
    df = _sample_df(pd.DatetimeIndex(idx))
    monkeypatch.setattr(client, "_download_data", lambda **kwargs: df)

    rows = client.fetch_ohlcv("SP500", "1M")
    assert len(rows) == 2
    dates = [pd.Timestamp(r["timestamp"], unit="s", tz="UTC") for r in rows]
    assert [d.day for d in dates] == [1, 1]
    assert all(d.hour == 0 for d in dates)
