from __future__ import annotations

import hashlib


def round_to_tick(price: float, tick_size: float) -> float:
    t = float(tick_size) if tick_size and float(tick_size) > 0 else 1e-8
    return round(round(float(price) / t) * t, 12)


def stable_level_id(
    *,
    symbol: str,
    level_type: str,
    layer: str | None,
    tier: str | None,
    price: float,
    tick_size: float,
) -> str:
    rounded = round_to_tick(price, tick_size)
    payload = f"{symbol}|{level_type}|{layer or ''}|{tier or ''}|{rounded:.12f}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"lvl_{digest}"

