"""
Прогон расчёта позиции через trading_bot.analytics.position_math (не ручной счёт).

Пример:
  python -m trading_bot.scripts.run_position_math --side long --entry 67941.7 --atr 2438.41 \\
    --risk 5 --stop-atr-mult 0.3 --tp1-atr 1 --tp2-atr 2 --tp3-atr 3 \\
    --tp1-pct 40 --tp2-pct 30 --tick 0.1 --qty-step 0.001
"""

from __future__ import annotations

import argparse
import json
import sys

from trading_bot.analytics.position_math import compute_position_plan, plan_to_dict


def main() -> int:
    p = argparse.ArgumentParser(description="Расчёт PositionPlan через position_math")
    p.add_argument("--side", choices=("long", "short"), required=True)
    p.add_argument("--entry", type=float, required=True, help="Цена входа (Y), сырая → MROUND по тику")
    p.add_argument("--base", type=float, default=None, help="База K (уровень)")
    p.add_argument("--atr", type=float, required=True)
    p.add_argument("--risk", type=float, required=True, help="Риск USDT (O)")
    p.add_argument("--stop-atr-mult", type=float, required=True)
    p.add_argument("--tp1-atr", type=float, default=1.0)
    p.add_argument("--tp2-atr", type=float, default=2.0)
    p.add_argument("--tp3-atr", type=float, default=3.0)
    p.add_argument("--tp1-pct", type=float, default=40.0)
    p.add_argument("--tp2-pct", type=float, default=30.0)
    p.add_argument("--use-entry-offset", action="store_true", help="Считать вход от уровня: Y = K +/- X")
    p.add_argument("--entry-offset-abs", type=float, default=0.0, help="Люфт X (абсолютное значение цены)")
    p.add_argument("--entry-offset-pct", type=float, default=None, help="Люфт X в % от K")
    p.add_argument(
        "--tp-in-stop-ranges",
        action="store_true",
        help="TP-множители интерпретировать как количество стоп-диапазонов",
    )
    p.add_argument("--tick", type=float, required=True, help="price_tick (W)")
    p.add_argument("--qty-step", type=float, required=True, help="qty_step (V)")
    p.add_argument("--min-qty", type=float, default=None)
    args = p.parse_args()

    base = args.base if args.base is not None else args.entry
    plan = compute_position_plan(
        side=args.side,
        base_price=base,
        entry_price_raw=args.entry,
        atr=args.atr,
        risk_usdt=args.risk,
        stop_atr_mult=args.stop_atr_mult,
        tp1_atr_mult=args.tp1_atr,
        tp2_atr_mult=args.tp2_atr,
        tp3_atr_mult=args.tp3_atr,
        tp1_share_pct=args.tp1_pct,
        tp2_share_pct=args.tp2_pct,
        price_tick=args.tick,
        qty_step=args.qty_step,
        entry_offset=args.entry_offset_abs,
        entry_offset_pct=args.entry_offset_pct,
        use_entry_offset=args.use_entry_offset,
        tp_in_stop_ranges=args.tp_in_stop_ranges,
        min_order_qty=args.min_qty,
    )
    print(json.dumps(plan_to_dict(plan), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
