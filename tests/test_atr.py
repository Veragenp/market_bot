"""Тесты ATR: стиль Герчика (всегда 10 свечей) и SMA(TR)."""

from trading_bot.analytics.atr import (
    GERCHIK_ATR_BARS,
    atr_gerchik_from_ohlcv_rows,
    atr_gerchik_style,
    atr_sma_last,
    atr_sma_last_from_ohlcv_rows,
)


def test_atr_gerchik_flat_ten_bars():
    high = [2.0] * GERCHIK_ATR_BARS
    low = [1.0] * GERCHIK_ATR_BARS
    v = atr_gerchik_style(high=high, low=low)
    assert v is not None
    assert abs(v - 1.0) < 1e-9


def test_atr_gerchik_drops_min_max_range():
    high = [2, 2, 2, 2, 2, 2, 2, 2, 102, 2.01]
    low = [1, 1, 1, 1, 1, 1, 1, 1, 2, 2.0]
    v = atr_gerchik_style(high=high, low=low)
    assert v is not None
    assert abs(v - 1.0) < 1e-6


def test_atr_gerchik_insufficient_bars():
    assert atr_gerchik_style(high=[1, 2], low=[0, 1]) is None
    assert atr_gerchik_style(high=[1] * 9, low=[0] * 9) is None


def test_atr_gerchik_from_ohlcv_rows():
    long = [{"high": 2.0, "low": 1.0, "close": 1.5} for _ in range(20)]
    v = atr_gerchik_from_ohlcv_rows(long)
    assert v is not None and abs(v - 1.0) < 1e-9


def test_atr_sma_last_flat_range():
    high = [2.0] * 14
    low = [1.0] * 14
    close = [1.5] * 14
    v = atr_sma_last(high=high, low=low, close=close, period=14)
    assert v is not None
    assert abs(v - 1.0) < 1e-9


def test_atr_sma_last_insufficient_bars():
    assert atr_sma_last(high=[1, 2], low=[0, 1], close=[1, 1], period=14) is None


def test_atr_sma_last_from_ohlcv_rows():
    rows = [
        {"high": 110.0, "low": 100.0, "close": 105.0},
        {"high": 120.0, "low": 108.0, "close": 115.0},
    ]
    assert atr_sma_last_from_ohlcv_rows(rows, period=5) is None
    long = [{"high": 2.0, "low": 1.0, "close": 1.5} for _ in range(20)]
    v = atr_sma_last_from_ohlcv_rows(long, period=14)
    assert v is not None and abs(v - 1.0) < 1e-9
