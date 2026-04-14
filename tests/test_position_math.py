"""Санити-тесты расчёта позиции по карте tutorial_v3."""

from __future__ import annotations

import math

import pytest

from trading_bot.analytics.position_math import compute_position_plan, mround


def test_mround():
    assert mround(10.04, 0.1) == pytest.approx(10.0)
    # 10.05/0.1 даёт tie → banker's round в Python может дать 10.0
    assert mround(10.06, 0.1) == pytest.approx(10.1)
    assert mround(3.14159, 0.01) == pytest.approx(3.14)


def test_long_plan_smoke():
    p = compute_position_plan(
        side="long",
        base_price=100_000.0,
        entry_price_raw=99_500.0,
        atr=500.0,
        risk_usdt=100.0,
        stop_atr_mult=1.0,
        tp1_atr_mult=1.0,
        tp2_atr_mult=2.0,
        tp3_atr_mult=3.0,
        tp1_share_pct=40.0,
        tp2_share_pct=30.0,
        price_tick=1.0,
        qty_step=0.001,
        use_entry_offset=False,
    )
    assert p.side == "long"
    assert p.entry_price == 99_500.0
    assert p.stop_price < p.entry_price
    assert p.tp1_price > p.entry_price
    assert p.qty_total > 0
    assert math.isclose(p.tp1_qty + p.tp2_qty + p.tp3_qty, p.qty_total, rel_tol=0, abs_tol=1e-9)
    assert p.rr_tp1 is not None and p.rr_tp1 > 0


def test_short_plan_smoke():
    p = compute_position_plan(
        side="short",
        base_price=100_000.0,
        entry_price_raw=100_200.0,
        atr=400.0,
        risk_usdt=50.0,
        stop_atr_mult=1.0,
        tp1_atr_mult=1.0,
        tp2_atr_mult=2.0,
        tp3_atr_mult=3.0,
        tp1_share_pct=40.0,
        tp2_share_pct=30.0,
        price_tick=1.0,
        qty_step=0.001,
        use_entry_offset=False,
    )
    assert p.stop_price > p.entry_price
    assert p.tp1_price < p.entry_price
    assert p.qty_total > 0


def test_tp_as_stop_ranges_and_entry_offset_pct():
    p = compute_position_plan(
        side="long",
        base_price=100.0,
        entry_price_raw=1.0,  # ignored: use_entry_offset=True → Y всегда от K
        atr=10.0,
        risk_usdt=5.0,
        stop_atr_mult=0.25,  # stop distance = 2.5
        tp1_atr_mult=3.0,  # 3 stop ranges => 0.75 ATR => 7.5
        tp2_atr_mult=3.0,
        tp3_atr_mult=3.0,
        tp1_share_pct=100.0,
        tp2_share_pct=0.0,
        price_tick=0.1,
        qty_step=0.01,
        entry_offset_pct=2.0,
        use_entry_offset=True,
        tp_in_stop_ranges=True,
    )
    # Люфт 2% от K=100 => X=2; Y = K + X = 102
    assert p.entry_price == pytest.approx(102.0)
    # AB = Y - 2.5 = 99.5
    assert p.stop_price == pytest.approx(99.5)
    # TP = Y + 7.5 = 109.5
    assert p.tp1_price == pytest.approx(109.5)


def test_use_entry_offset_zero_entry_is_level_k_not_raw():
    """Лимит на уровне K; сырая цена в entry_price_raw не влияет."""
    p = compute_position_plan(
        side="long",
        base_price=0.926,
        entry_price_raw=1.5,
        atr=0.02,
        risk_usdt=1.0,
        stop_atr_mult=0.2,
        tp1_atr_mult=3.0,
        tp2_atr_mult=3.0,
        tp3_atr_mult=3.0,
        tp1_share_pct=100.0,
        tp2_share_pct=0.0,
        price_tick=0.0001,
        qty_step=0.1,
        entry_offset_pct=0.0,
        use_entry_offset=True,
        tp_in_stop_ranges=True,
    )
    assert p.base_price == pytest.approx(0.926)
    assert p.entry_price == pytest.approx(0.926)
