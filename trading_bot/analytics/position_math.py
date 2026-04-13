"""
Расчёт плана позиции по карте `docs/tutorial_v3_long_short_formula_map.md` (GoogleTest.xlsx).

Округления: MROUND по цене (тик) и по объёму (шаг количества).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

Side = Literal["long", "short"]


def mround(x: float, m: float) -> float:
    """Как Excel MROUND: ближайшее кратное m (m > 0)."""
    if m <= 0:
        raise ValueError("mround: m must be positive")
    n = round(x / m)
    return float(n * m)


@dataclass(frozen=True)
class PositionPlan:
    side: Side
    base_price: float
    entry_price: float
    stop_price: float
    qty_total: float
    notional_usdt: float
    required_notional_s: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    tp1_qty: float
    tp2_qty: float
    tp3_qty: float
    rr_tp1: Optional[float]
    rr_tp2: Optional[float]
    tp1_profit_usdt: float
    tp2_profit_usdt: float
    tp3_profit_usdt: float
    pnl_all_tp_usdt: float
    profit_to_risk_all: Optional[float]


def compute_position_plan(
    *,
    side: Side,
    base_price: float,
    entry_price_raw: float,
    atr: float,
    risk_usdt: float,
    stop_atr_mult: float,
    tp1_atr_mult: float,
    tp2_atr_mult: float,
    tp3_atr_mult: float,
    tp1_share_pct: float,
    tp2_share_pct: float,
    price_tick: float,
    qty_step: float,
    entry_offset: float = 0.0,
    entry_offset_pct: Optional[float] = None,
    use_entry_offset: bool = False,
    tp_in_stop_ranges: bool = False,
    min_order_qty: Optional[float] = None,
) -> PositionPlan:
    """
    K = base_price, Y = entry (смещение X или сырая цена), M = atr.
    Если tp_in_stop_ranges=True, TP-множители трактуются как число стоп-диапазонов
    (т.е. effective_tp_atr_mult = stop_atr_mult * tp_atr_mult).
    """
    if atr <= 0 or risk_usdt <= 0:
        raise ValueError("atr and risk_usdt must be positive")
    if price_tick <= 0 or qty_step <= 0:
        raise ValueError("price_tick and qty_step must be positive")

    k = float(base_price)
    x = float(entry_offset)
    if entry_offset_pct is not None and x == 0.0:
        # Люфт в процентах задаём от ATR (а не от K), чтобы +5% означало 0.05 * ATR.
        x = float(atr) * float(entry_offset_pct) / 100.0
    if use_entry_offset and x != 0:
        if side == "long":
            y = mround(k + x, price_tick)
        else:
            y = mround(k - x, price_tick)
    else:
        y = mround(float(entry_price_raw), price_tick)

    m_stop = float(atr) * float(stop_atr_mult)
    if side == "long":
        ab = mround(y - m_stop, price_tick)
        if not (ab < y):
            raise ValueError("long: stop must be below entry (check atr/mult/tick)")
        denom_s = 100.0 - (ab * 100.0 / y)
        if denom_s <= 0:
            raise ValueError("long: invalid stop vs entry for risk notional")
        s = 100.0 * risk_usdt / denom_s
    else:
        ab = mround(y + m_stop, price_tick)
        if not (ab > y):
            raise ValueError("short: stop must be above entry")
        denom_s = (ab * 100.0 / y) - 100.0
        if denom_s <= 0:
            raise ValueError("short: invalid stop vs entry for risk notional")
        s = 100.0 * risk_usdt / denom_s

    u = mround(s / y, qty_step)
    if min_order_qty is not None and u < float(min_order_qty):
        u = mround(float(min_order_qty), qty_step)
        if u < float(min_order_qty):
            u = float(min_order_qty)

    z = u
    t_notional = u * y

    tp1_eff = float(tp1_atr_mult)
    tp2_eff = float(tp2_atr_mult)
    tp3_eff = float(tp3_atr_mult)
    if tp_in_stop_ranges:
        tp1_eff *= float(stop_atr_mult)
        tp2_eff *= float(stop_atr_mult)
        tp3_eff *= float(stop_atr_mult)

    if side == "long":
        aa = mround(y + atr * tp1_eff, price_tick)
        am = mround(y + atr * tp2_eff, price_tick)
        av = mround(y + atr * tp3_eff, price_tick)
    else:
        aa = mround(y - atr * tp1_eff, price_tick)
        am = mround(y - atr * tp2_eff, price_tick)
        av = mround(y - atr * tp3_eff, price_tick)

    ac = mround(z * tp1_share_pct / 100.0, qty_step)
    ak = mround(z * tp2_share_pct / 100.0, qty_step)
    at_rest = z - ac - ak
    if at_rest < 0:
        raise ValueError("tp share %%: remainder negative after TP1/TP2")

    # R:R: long TP1 (AA-Y)/(Y-AB); long TP2 (AM-Y)/(Y-AB) как в карте (AO); short — зеркально
    if side == "long":
        d_risk = y - ab
        rr1 = (aa - y) / d_risk if d_risk else None
        rr2 = (am - y) / d_risk if d_risk else None
    else:
        d_risk = ab - y
        rr1 = (y - aa) / d_risk if d_risk else None
        rr2 = (y - am) / d_risk if d_risk else None

    if side == "long":
        p1 = (aa - y) * ac
        p2 = (am - y) * ak
        p3 = (av - y) * at_rest
    else:
        p1 = (y - aa) * ac
        p2 = (y - am) * ak
        p3 = (y - av) * at_rest

    pnl_all = p1 + p2 + p3
    pr = (pnl_all / risk_usdt) if risk_usdt else None

    return PositionPlan(
        side=side,
        base_price=k,
        entry_price=y,
        stop_price=ab,
        qty_total=u,
        notional_usdt=t_notional,
        required_notional_s=s,
        tp1_price=aa,
        tp2_price=am,
        tp3_price=av,
        tp1_qty=ac,
        tp2_qty=ak,
        tp3_qty=at_rest,
        rr_tp1=rr1,
        rr_tp2=rr2,
        tp1_profit_usdt=p1,
        tp2_profit_usdt=p2,
        tp3_profit_usdt=p3,
        pnl_all_tp_usdt=pnl_all,
        profit_to_risk_all=pr,
    )


def plan_to_dict(p: PositionPlan) -> dict:
    return {
        "side": p.side,
        "base_price": p.base_price,
        "entry_price": p.entry_price,
        "stop_price": p.stop_price,
        "qty_total": p.qty_total,
        "notional_usdt": p.notional_usdt,
        "required_notional_s": p.required_notional_s,
        "tp1_price": p.tp1_price,
        "tp2_price": p.tp2_price,
        "tp3_price": p.tp3_price,
        "tp1_qty": p.tp1_qty,
        "tp2_qty": p.tp2_qty,
        "tp3_qty": p.tp3_qty,
        "rr_tp1": p.rr_tp1,
        "rr_tp2": p.rr_tp2,
        "tp1_profit_usdt": p.tp1_profit_usdt,
        "tp2_profit_usdt": p.tp2_profit_usdt,
        "tp3_profit_usdt": p.tp3_profit_usdt,
        "pnl_all_tp_usdt": p.pnl_all_tp_usdt,
        "profit_to_risk_all": p.profit_to_risk_all,
    }


__all__ = ["PositionPlan", "compute_position_plan", "mround", "plan_to_dict"]
