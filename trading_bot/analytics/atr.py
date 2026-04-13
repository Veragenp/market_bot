"""
ATR для торгового контура: **только стиль Герчика** (см. ниже) — единственное определение, которое
пишется в `instruments.atr` (ежедневно из `DataLoaderManager.update_instruments_atr_for_trading_symbols`)
и читается из БД во всех модулях (cycle_levels, VP, level_events, human levels batch и т.д.).

Функции `true_range_*` / `atr_sma_*` оставлены как вспомогательные (тесты, внешние сравнения);
**не используйте их** для заполнения `instruments` или для масштабов уровней в боте.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Для ATR в `instruments`: ровно последние 10 дневных свечей (не параметризуется).
GERCHIK_ATR_BARS = 10


def atr_gerchik_style(*, high: List[float], low: List[float]) -> Optional[float]:
    """
    По Герчику на фиксированном окне `GERCHIK_ATR_BARS` (10): размах (H−L) по последним 10 барам,
    сортировка, отбрасываем одну минимальную и одну максимальную свечу, среднее по остальным.
    Без учёта закрытий (логика «без гэпов» как в крипте).
    """
    period = GERCHIK_ATR_BARS
    if len(high) < period or len(low) < period:
        return None

    last_high = high[-period:]
    last_low = low[-period:]

    ranges = [h - l for h, l in zip(last_high, last_low)]
    ranges.sort()
    clean_ranges = ranges[1:-1]
    if not clean_ranges:
        return None
    return sum(clean_ranges) / float(len(clean_ranges))


def atr_gerchik_from_ohlcv_rows(rows: List[Dict[str, Any]]) -> Optional[float]:
    """По строкам OHLCV из БД (например `get_ohlcv`): поля high, low, порядок по возрастанию timestamp."""
    if not rows:
        return None
    try:
        high = [float(r["high"]) for r in rows]
        low = [float(r["low"]) for r in rows]
    except (KeyError, TypeError, ValueError):
        return None
    return atr_gerchik_style(high=high, low=low)


def true_range_series(high: List[float], low: List[float], close: List[float]) -> List[float]:
    """True Range по ряду баров по возрастанию времени. Первый бар: H−L."""
    if not high or len(high) != len(low) or len(low) != len(close):
        return []
    tr: List[float] = [high[0] - low[0]]
    for i in range(1, len(high)):
        pc = close[i - 1]
        tr.append(
            max(
                high[i] - low[i],
                abs(high[i] - pc),
                abs(low[i] - pc),
            )
        )
    return tr


def atr_sma_last(*, high: List[float], low: List[float], close: List[float], period: int = 14) -> Optional[float]:
    """Классика: среднее TR за последние `period` баров (включая гэпы через prev close)."""
    if period < 1:
        return None
    tr = true_range_series(high, low, close)
    if len(tr) < period:
        return None
    window = tr[-period:]
    return sum(window) / float(period)


def atr_sma_last_from_ohlcv_rows(rows: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """SMA(TR) по строкам OHLCV (если нужен не стиль Герчика)."""
    if not rows:
        return None
    try:
        high = [float(r["high"]) for r in rows]
        low = [float(r["low"]) for r in rows]
        close = [float(r["close"]) for r in rows]
    except (KeyError, TypeError, ValueError):
        return None
    return atr_sma_last(high=high, low=low, close=close, period=period)
