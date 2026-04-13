"""
Человеческие уровни (v1): чистые функции без записи в БД.

ТФ: только 1d и 1w. Фракталы Bill Williams 2+2, кластеризация по ширине в единицах ATR,
flip по правилам v1 + ретест только со «входом с нужной стороны».
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

import numpy as np
import pandas as pd

from trading_bot.analytics.atr import GERCHIK_ATR_BARS, atr_gerchik_from_ohlcv_rows

Timeframe = Literal["1d", "1w"]

# Веса фракталов при суммировании силы зоны (W1/D1)
FRACTAL_WEIGHT_D1 = 1.0
FRACTAL_WEIGHT_W1 = 3.0

# ATR_d1 (Gerchik из БД или хвост D1) × √5 для масштаба недельной зоны
W1_ATR_EQUIV_MULT = 2.23606797749979

# Кластеризация: max(span зоны) <= cluster_atr_mult * ATR_масштаба
DEFAULT_CLUSTER_ATR_MULT = 0.25


@dataclass(frozen=True)
class HumanFractal:
    """Подтверждённый фрактал (центр на баре i, цена = high или low этого бара)."""

    bar_index: int
    timestamp: Optional[int]
    price: float
    kind: Literal["high", "low"]
    timeframe: Timeframe
    weight: float


@dataclass(frozen=True)
class HumanZone:
    zone_low: float
    zone_high: float
    timeframe: Timeframe
    strength: float
    fractal_count: int

    @property
    def center(self) -> float:
        return (self.zone_low + self.zone_high) / 2.0


@dataclass(frozen=True)
class FlipEvent:
    """Зафиксированный flip на закрытиях того же ТФ, что и зона."""

    bar_index_confirm: int
    timestamp: Optional[int]
    direction: Literal["resistance_to_support", "support_to_resistance"]


@dataclass
class HumanLevelsResult:
    """Итог пайплайна: зоны по D1/W1 и метрики ATR (Gerchik, как в `instruments`)."""

    zones_d1: list[HumanZone] = field(default_factory=list)
    zones_w1: list[HumanZone] = field(default_factory=list)
    atr_d1_last: float = 0.0
    atr_w1_equiv: float = 0.0
    fractals_d1: list[HumanFractal] = field(default_factory=list)
    fractals_w1: list[HumanFractal] = field(default_factory=list)


def filter_human_zones(
    zones: list[HumanZone],
    *,
    min_fractal_count: int = 1,
    min_strength: float = 0.0,
) -> list[HumanZone]:
    """Оставляет зоны с fractal_count >= порога и (если min_strength > 0) strength >= порога."""
    if min_fractal_count < 1:
        min_fractal_count = 1
    if min_fractal_count <= 1 and min_strength <= 0.0:
        return list(zones)
    out: list[HumanZone] = []
    for z in zones:
        if z.fractal_count < min_fractal_count:
            continue
        if min_strength > 0.0 and z.strength < min_strength:
            continue
        out.append(z)
    return out


def deduplicate_zones_by_vertical_gap(
    zones: list[HumanZone],
    atr_value: float,
    min_gap_atr: float,
) -> list[HumanZone]:
    """
    Жадное разрежение по вертикали: оставляем сильные зоны, отбрасываем те, чей центр
    ближе min_gap_atr * ATR к центру уже принятой. Только для списка одного ТФ (ожидается D1).
    min_gap_atr <= 0 или невалидный ATR — без изменений.
    """
    if min_gap_atr <= 0.0 or not zones:
        return list(zones)
    av = float(atr_value)
    if not np.isfinite(av) or av <= 0.0:
        return list(zones)
    min_gap_price = float(min_gap_atr) * av
    sorted_zones = sorted(zones, key=lambda z: z.strength, reverse=True)
    filtered: list[HumanZone] = []
    for z in sorted_zones:
        cz = z.center
        if not filtered:
            filtered.append(z)
            continue
        too_close = any(abs(cz - a.center) < min_gap_price for a in filtered)
        if not too_close:
            filtered.append(z)
    return filtered


def wilder_atr(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """Wilder ATR(period); первые (period-1) значений — nan."""
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)
    n = len(close)
    if n == 0:
        return np.array([])
    prev_close = np.empty(n, dtype=float)
    prev_close[0] = close[0]
    prev_close[1:] = close[:-1]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr = np.full(n, np.nan, dtype=float)
    if n < period:
        return atr
    atr[period - 1] = float(np.mean(tr[:period]))
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def atr_w1_equiv_from_daily(atr_d1: float) -> float:
    return float(atr_d1) * W1_ATR_EQUIV_MULT


def bill_williams_fractal_mask(
    high: np.ndarray,
    low: np.ndarray,
    left: int = 2,
    right: int = 2,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Фрактал вверх на i: high[i] строго выше максимумов left баров слева и right справа (не включая i).
    Фрактал вниз симметрично по low.
    """
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    n = len(high)
    up = np.zeros(n, dtype=bool)
    dn = np.zeros(n, dtype=bool)
    for i in range(left, n - right):
        if high[i] > np.max(high[i - left : i]) and high[i] > np.max(high[i + 1 : i + right + 1]):
            up[i] = True
        if low[i] < np.min(low[i - left : i]) and low[i] < np.min(low[i + 1 : i + right + 1]):
            dn[i] = True
    return up, dn


def extract_fractals(
    df: pd.DataFrame,
    *,
    timeframe: Timeframe,
    weight: float,
) -> list[HumanFractal]:
    """df: колонки high, low; опционально timestamp."""
    if df is None or df.empty or len(df) < 5:
        return []
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    ts_col = df["timestamp"].to_numpy(dtype=np.int64) if "timestamp" in df.columns else None
    up, dn = bill_williams_fractal_mask(high, low)
    out: list[HumanFractal] = []
    for i in np.flatnonzero(up):
        ts = int(ts_col[i]) if ts_col is not None else None
        out.append(
            HumanFractal(
                bar_index=int(i),
                timestamp=ts,
                price=float(high[i]),
                kind="high",
                timeframe=timeframe,
                weight=float(weight),
            )
        )
    for i in np.flatnonzero(dn):
        ts = int(ts_col[i]) if ts_col is not None else None
        out.append(
            HumanFractal(
                bar_index=int(i),
                timestamp=ts,
                price=float(low[i]),
                kind="low",
                timeframe=timeframe,
                weight=float(weight),
            )
        )
    out.sort(key=lambda f: f.bar_index)
    return out


def cluster_fractal_prices_to_zones(
    fractals: list[HumanFractal],
    *,
    eps_price: float,
    timeframe: Timeframe,
) -> list[HumanZone]:
    """
    Сортировка по цене, цепочное объединение: (max - min) в кластере <= eps_price.
    """
    if not fractals or eps_price <= 0:
        return []
    pts = [(f.price, f.weight) for f in fractals]
    pts.sort(key=lambda x: x[0])
    clusters: list[list[tuple[float, float]]] = []
    for price, w in pts:
        if not clusters:
            clusters.append([(price, w)])
            continue
        lo = clusters[-1][0][0]
        if price - lo <= eps_price:
            clusters[-1].append((price, w))
        else:
            clusters.append([(price, w)])
    zones: list[HumanZone] = []
    for cl in clusters:
        prices = [p for p, _ in cl]
        weights = [w for _, w in cl]
        zones.append(
            HumanZone(
                zone_low=float(min(prices)),
                zone_high=float(max(prices)),
                timeframe=timeframe,
                strength=float(sum(weights)),
                fractal_count=len(cl),
            )
        )
    zones.sort(key=lambda z: z.zone_low)
    return zones


def gerchik_atr_from_d1_df(df_d1: pd.DataFrame) -> float:
    """
    Тот же Gerchik, что в `atr.py` / `instruments.atr`, по последним GERCHIK_ATR_BARS дневным барам.
    Fallback, если для символа ещё нет строки в `instruments` (например макро/индексы).
    """
    if df_d1 is None or df_d1.empty or len(df_d1) < GERCHIK_ATR_BARS:
        return 0.0
    if "high" not in df_d1.columns or "low" not in df_d1.columns:
        return 0.0
    tail = df_d1.tail(GERCHIK_ATR_BARS)
    rows = tail[["high", "low"]].to_dict("records")
    v = atr_gerchik_from_ohlcv_rows(rows)
    return float(v) if v is not None else 0.0


def last_valid_atr_d1(df_d1: pd.DataFrame, *, atr_period: int = 14) -> float:
    """Устарело: для пайплайна используйте `gerchik_atr_from_d1_df` или `atr_d1` из БД. Оставлено для тестов Wilder."""
    if df_d1 is None or df_d1.empty:
        return 0.0
    high = df_d1["high"].to_numpy(dtype=float)
    low = df_d1["low"].to_numpy(dtype=float)
    close = df_d1["close"].to_numpy(dtype=float)
    atr = wilder_atr(high, low, close, atr_period)
    if not np.any(np.isfinite(atr)):
        return 0.0
    idx = np.where(np.isfinite(atr))[0]
    return float(atr[idx[-1]])


def build_zones_for_timeframe(
    df: pd.DataFrame,
    *,
    timeframe: Timeframe,
    fractal_weight: float,
    cluster_eps_price: float,
) -> tuple[list[HumanZone], list[HumanFractal]]:
    """
    Фракталы с df данного ТФ; ширина кластера cluster_eps_price задаётся снаружи.
    Для W1 зон eps = cluster_atr_mult * ATR_d1 * √5 (передаётся в cluster_eps_price).
    """
    if df is None or df.empty:
        return [], []
    fr = extract_fractals(df, timeframe=timeframe, weight=fractal_weight)
    if cluster_eps_price <= 0:
        return [], fr
    zones = cluster_fractal_prices_to_zones(fr, eps_price=cluster_eps_price, timeframe=timeframe)
    return zones, fr


def close_in_zone(c: float, zl: float, zh: float) -> bool:
    return zl <= c <= zh


def detect_flip_events(
    close: np.ndarray,
    zone_low: float,
    zone_high: float,
    *,
    timestamps: Optional[np.ndarray] = None,
) -> list[FlipEvent]:
    """
    Резист → поддержка:
      1) два подряд close > zone_high
      2) ретест: close в [zone_low, zone_high] и close_prev < zone_low
      3) следующая свеча close > zone_high
    Поддержка → сопротивление — зеркально (close_prev > zone_high, затем close < zone_low).
    """
    close = np.asarray(close, dtype=float)
    n = len(close)
    if n < 4:
        return []
    ts_arr = timestamps
    events: list[FlipEvent] = []

    i = 1
    while i < n - 2:
        progressed = False

        if close[i - 1] > zone_high and close[i] > zone_high:
            j = i + 1
            while j < n - 1:
                c_prev = float(close[j - 1])
                c = float(close[j])
                if close_in_zone(c, zone_low, zone_high) and c_prev < zone_low:
                    if float(close[j + 1]) > zone_high:
                        tsc = int(ts_arr[j + 1]) if ts_arr is not None else None
                        events.append(
                            FlipEvent(
                                bar_index_confirm=j + 1,
                                timestamp=tsc,
                                direction="resistance_to_support",
                            )
                        )
                        i = j + 2
                        progressed = True
                        break
                j += 1

        if not progressed and close[i - 1] < zone_low and close[i] < zone_low:
            j = i + 1
            while j < n - 1:
                c_prev = float(close[j - 1])
                c = float(close[j])
                if close_in_zone(c, zone_low, zone_high) and c_prev > zone_high:
                    if float(close[j + 1]) < zone_low:
                        tsc = int(ts_arr[j + 1]) if ts_arr is not None else None
                        events.append(
                            FlipEvent(
                                bar_index_confirm=j + 1,
                                timestamp=tsc,
                                direction="support_to_resistance",
                            )
                        )
                        i = j + 2
                        progressed = True
                        break
                j += 1

        if not progressed:
            i += 1

    return events


def run_human_levels_pipeline(
    df_d1: pd.DataFrame,
    df_w1: pd.DataFrame,
    *,
    atr_d1: Optional[float] = None,
    cluster_atr_mult: float = DEFAULT_CLUSTER_ATR_MULT,
    zone_min_gap_atr_d1: float = 0.0,
) -> HumanLevelsResult:
    """
    Полный прогон v1: масштаб — **Gerchik** как в `instruments.atr`.
    Если передан `atr_d1` (>0), берётся он (типично из БД); иначе — Gerchik по хвосту `df_d1`.
    Кластер W1: ATR_W1_equiv = ATR_d1 × √5.
    """
    if atr_d1 is not None and float(atr_d1) > 0:
        atr_d = float(atr_d1)
    else:
        atr_d = gerchik_atr_from_d1_df(df_d1)
    eps_d1 = float(cluster_atr_mult) * atr_d if atr_d > 0 else 0.0
    atr_w1_eq = atr_w1_equiv_from_daily(atr_d) if atr_d > 0 else 0.0
    eps_w1 = float(cluster_atr_mult) * atr_w1_eq if atr_w1_eq > 0 else 0.0

    zones_d1, fr_d1 = build_zones_for_timeframe(
        df_d1,
        timeframe="1d",
        fractal_weight=FRACTAL_WEIGHT_D1,
        cluster_eps_price=eps_d1,
    )
    zones_d1 = deduplicate_zones_by_vertical_gap(zones_d1, atr_d, float(zone_min_gap_atr_d1))
    zones_w1, fr_w1 = build_zones_for_timeframe(
        df_w1,
        timeframe="1w",
        fractal_weight=FRACTAL_WEIGHT_W1,
        cluster_eps_price=eps_w1,
    )
    return HumanLevelsResult(
        zones_d1=zones_d1,
        zones_w1=zones_w1,
        atr_d1_last=atr_d,
        atr_w1_equiv=atr_w1_eq,
        fractals_d1=fr_d1,
        fractals_w1=fr_w1,
    )


def zones_to_jsonable(result: HumanLevelsResult) -> dict[str, Any]:
    """Сериализация без ORM (бот/лог)."""

    def zlist(zs: list[HumanZone]) -> list[dict[str, Any]]:
        return [
            {
                "zone_low": z.zone_low,
                "zone_high": z.zone_high,
                "timeframe": z.timeframe,
                "strength": z.strength,
                "fractal_count": z.fractal_count,
            }
            for z in zs
        ]

    return {
        "zones_d1": zlist(result.zones_d1),
        "zones_w1": zlist(result.zones_w1),
        "atr_d1_last": result.atr_d1_last,
        "atr_w1_equiv": result.atr_w1_equiv,
        "fractals_d1_count": len(result.fractals_d1),
        "fractals_w1_count": len(result.fractals_w1),
    }


def human_levels_from_ohlcv_rows(
    d1_rows: list[dict[str, Any]],
    w1_rows: list[dict[str, Any]],
    **kwargs: Any,
) -> HumanLevelsResult:
    """Опционально: списки записей как из get_ohlcv → DataFrame → пайплайн."""

    def _df(rows: list[dict[str, Any]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    kwargs.pop("atr_period", None)
    return run_human_levels_pipeline(_df(d1_rows), _df(w1_rows), **kwargs)


__all__ = [
    "FRACTAL_WEIGHT_D1",
    "FRACTAL_WEIGHT_W1",
    "W1_ATR_EQUIV_MULT",
    "DEFAULT_CLUSTER_ATR_MULT",
    "FlipEvent",
    "HumanFractal",
    "HumanLevelsResult",
    "HumanZone",
    "Timeframe",
    "atr_w1_equiv_from_daily",
    "bill_williams_fractal_mask",
    "build_zones_for_timeframe",
    "close_in_zone",
    "cluster_fractal_prices_to_zones",
    "deduplicate_zones_by_vertical_gap",
    "detect_flip_events",
    "extract_fractals",
    "filter_human_zones",
    "gerchik_atr_from_d1_df",
    "human_levels_from_ohlcv_rows",
    "last_valid_atr_d1",
    "run_human_levels_pipeline",
    "wilder_atr",
    "zones_to_jsonable",
]
