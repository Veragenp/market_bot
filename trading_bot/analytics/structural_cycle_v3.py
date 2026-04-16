from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from trading_bot.config import settings as settings_pkg
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur

logger = logging.getLogger(__name__)


@dataclass
class Level:
    price: float
    strength: float
    level_id: Optional[int] = None


@dataclass
class Channel:
    low_level: Level
    high_level: Level
    width_atr: float
    low_zone_high: float
    high_zone_low: float


class MarketSyncDetector:
    def __init__(
        self,
        min_width_atr: float = 0.7,
        max_width_atr: float = 1.5,
        zone_tolerance_atr: float = 0.25,
        sync_threshold: float = 0.7,
        max_opposite_ratio: float = 0.3,
    ):
        self.min_width_atr = min_width_atr
        self.max_width_atr = max_width_atr
        self.zone_tolerance_atr = zone_tolerance_atr
        self.sync_threshold = sync_threshold
        self.max_opposite_ratio = max_opposite_ratio

    def get_channels_for_symbol(self, levels: Sequence[Level], atr: float) -> List[Channel]:
        if len(levels) < 2 or atr <= 0:
            return []
        channels: List[Channel] = []
        sorted_levels = sorted(levels, key=lambda l: l.price)
        for i in range(len(sorted_levels) - 1):
            low_lvl = sorted_levels[i]
            high_lvl = sorted_levels[i + 1]
            width_atr = (high_lvl.price - low_lvl.price) / atr
            if self.min_width_atr <= width_atr <= self.max_width_atr:
                tolerance = self.zone_tolerance_atr * atr
                channels.append(
                    Channel(
                        low_level=low_lvl,
                        high_level=high_lvl,
                        width_atr=width_atr,
                        low_zone_high=low_lvl.price + tolerance,
                        high_zone_low=high_lvl.price - tolerance,
                    )
                )
        return channels

    def get_zone_for_price(self, price: float, channels: List[Channel], atr: float) -> Optional[str]:
        """
        Определяет зону для текущей цены.
        Сначала ищет канал, в который цена попадает с учётом tolerance.
        Если таких нет – выбирает ближайший канал и возвращает 'low' (если цена ниже) или 'high' (если выше).
        """
        if not channels or atr <= 0:
            return None

        tolerance = self.zone_tolerance_atr * atr

        # 1. Ищем канал, в котором цена находится внутри расширенного диапазона
        for ch in channels:
            low_ext = ch.low_level.price - tolerance
            high_ext = ch.high_level.price + tolerance
            if low_ext <= price <= high_ext:
                # Цена внутри расширенного канала – определяем зону
                if price <= ch.low_level.price + tolerance:
                    return "low"
                if price >= ch.high_level.price - tolerance:
                    return "high"
                return "mid"

        # 2. Если ни один канал не содержит цену, находим ближайший по расстоянию до границ
        best_ch = None
        best_dist = float("inf")
        for ch in channels:
            if price < ch.low_level.price:
                dist = ch.low_level.price - price
            elif price > ch.high_level.price:
                dist = price - ch.high_level.price
            else:
                # Теоретически сюда не попадём, но на всякий случай
                dist = 0
            if dist < best_dist:
                best_dist = dist
                best_ch = ch

        if best_ch:
            if price < best_ch.low_level.price:
                return "low"
            else:
                return "high"

        return None

    def compute_distribution(self, symbols_data: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, int], int]:
        zones = {"low": 0, "mid": 0, "high": 0, "invalid": 0}
        for _sym, data in symbols_data.items():
            price = data.get("price")
            atr = data.get("atr")
            levels = data.get("levels", [])
            if price is None or atr is None or atr <= 0 or not levels:
                zones["invalid"] += 1
                continue
            channels = self.get_channels_for_symbol(levels, atr)
            zone = self.get_zone_for_price(float(price), channels, float(atr))
            if zone is None:
                zones["invalid"] += 1
            else:
                zones[zone] += 1
        total_valid = len(symbols_data) - zones["invalid"]
        return zones, total_valid

    def get_synced_direction(self, symbols_data: Dict[str, Dict[str, Any]]) -> Optional[str]:
        zones, total_valid = self.compute_distribution(symbols_data)
        if total_valid == 0:
            logger.warning("V3: no valid symbols for synchronization")
            return None
        low_cnt = zones["low"]
        mid_cnt = zones["mid"]
        high_cnt = zones["high"]
        low_mid_ratio = (low_cnt + mid_cnt) / total_valid
        high_mid_ratio = (high_cnt + mid_cnt) / total_valid
        high_ratio = high_cnt / total_valid
        low_ratio = low_cnt / total_valid
        if low_mid_ratio >= self.sync_threshold and high_ratio <= self.max_opposite_ratio:
            return "long"
        if high_mid_ratio >= self.sync_threshold and low_ratio <= self.max_opposite_ratio:
            return "short"
        return None


def _fetch_ref_price(cur, symbol: str) -> Optional[float]:
    row = cur.execute(
        """
        SELECT close FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        ORDER BY timestamp DESC LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if row is None or row["close"] is None:
        return None
    ref = float(row["close"])
    return ref if ref > 0 else None


def _fetch_levels(cur, symbol: str, allowed_types: Sequence[str], limit: int) -> List[Level]:
    if not allowed_types:
        return []
    ph = ",".join("?" for _ in allowed_types)
    rows = cur.execute(
        f"""
        SELECT id, price, COALESCE(volume_peak, strength, 0) AS lvl_strength
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
        ORDER BY COALESCE(volume_peak, strength, 0) DESC, COALESCE(updated_at, created_at) DESC
        LIMIT ?
        """,
        (symbol, *allowed_types, int(limit)),
    ).fetchall()
    out = [
        Level(
            price=float(r["price"]),
            strength=float(r["lvl_strength"] or 0.0),
            level_id=int(r["id"]) if r["id"] is not None else None,
        )
        for r in rows
        if r["price"] is not None
    ]
    return sorted(out, key=lambda x: x.price)


def _pick_display_channel(price: float, channels: Sequence[Channel]) -> Optional[Channel]:
    if not channels:
        return None
    inside = [ch for ch in channels if ch.low_level.price <= price <= ch.high_level.price]
    pool = inside if inside else list(channels)
    return max(pool, key=lambda ch: ch.low_level.strength + ch.high_level.strength)


def build_structural_v3_report_df(cur, symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    syms = list(symbols) if symbols is not None else list(TRADING_SYMBOLS)
    allowed_types = tuple(settings_pkg.STRUCTURAL_ALLOWED_LEVEL_TYPES)
    top_k = int(settings_pkg.STRUCTURAL_TOP_K)
    detector = MarketSyncDetector(
        min_width_atr=float(settings_pkg.STRUCTURAL_W_MIN),
        max_width_atr=float(settings_pkg.STRUCTURAL_W_MAX),
        zone_tolerance_atr=0.25,
    )

    symbols_data: Dict[str, Dict[str, Any]] = {}
    rows_out: List[Dict[str, Any]] = []
    exported_at = datetime.now(timezone.utc).isoformat()

    for sym in syms:
        ref_price = _fetch_ref_price(cur, sym)
        atr = get_instruments_atr_bybit_futures_cur(cur, sym)
        levels = _fetch_levels(cur, sym, allowed_types, max(2, top_k * 4))
        symbols_data[sym] = {"price": ref_price, "atr": atr, "levels": levels}
        channels = detector.get_channels_for_symbol(levels, float(atr or 0.0)) if atr else []
        zone = (
            detector.get_zone_for_price(float(ref_price), channels, float(atr))
            if (ref_price is not None and atr is not None and channels)
            else None
        )
        chosen = _pick_display_channel(float(ref_price), channels) if (ref_price is not None and channels) else None
        rows_out.append(
            {
                "exported_at_utc": exported_at,
                "symbol": sym,
                "ref_price": ref_price,
                "atr_daily": float(atr) if atr is not None else None,
                "levels_n": len(levels),
                "channels_n": len(channels),
                "zone_v3": zone or "invalid",
                "channel_low_price": chosen.low_level.price if chosen else None,
                "channel_high_price": chosen.high_level.price if chosen else None,
                "channel_width_atr": chosen.width_atr if chosen else None,
                "channel_low_strength": chosen.low_level.strength if chosen else None,
                "channel_high_strength": chosen.high_level.strength if chosen else None,
                "channel_low_level_id": chosen.low_level.level_id if chosen else None,
                "channel_high_level_id": chosen.high_level.level_id if chosen else None,
            }
        )

    synced = detector.get_synced_direction(symbols_data)
    zones, total_valid = detector.compute_distribution(symbols_data)
    for r in rows_out:
        r["sync_direction_v3"] = synced or ""
        r["sync_threshold"] = detector.sync_threshold
        r["max_opposite_ratio"] = detector.max_opposite_ratio
        r["total_valid_symbols"] = total_valid
        r["zones_low"] = zones["low"]
        r["zones_mid"] = zones["mid"]
        r["zones_high"] = zones["high"]
        r["zones_invalid"] = zones["invalid"]

    return pd.DataFrame(rows_out)


__all__ = [
    "Level",
    "Channel",
    "MarketSyncDetector",
    "build_structural_v3_report_df",
]
