"""
Проверка structural-модуля на текущей БД (инварианты после скана).

  PYTHONPATH=. python -m trading_bot.scripts.verify_structural

Не трогает cycle_levels (auto_freeze=False). Код возврата 0 — ок, 1 — есть нарушения.

Полный realtime (touch_window → entry_timer → freeze) покрыт в tests/test_structural_cycle.py
и требует либо долгого живого прогона, либо price_ticks_override как в тестах.
"""

from __future__ import annotations

import os
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.config import settings as st
from trading_bot.data.db import get_connection
from trading_bot.data.structural_cycle_db import run_structural_pipeline


def main() -> int:
    print("=== structural scan (dry, no freeze) ===")
    print(
        f"settings: min_side={st.STRUCTURAL_MIN_CANDIDATES_PER_SIDE} "
        f"min_pool={st.STRUCTURAL_MIN_POOL_SYMBOLS} "
        f"ref={st.STRUCTURAL_REF_PRICE_SOURCE} "
        f"types={st.STRUCTURAL_ALLOWED_LEVEL_TYPES}"
    )
    r = run_structural_pipeline(auto_freeze=False)
    cid = r.get("structural_cycle_id")
    print("pipeline:", r)

    if r.get("error"):
        print("FAIL: pipeline error")
        return 1
    if not cid:
        print("FAIL: no cycle_id")
        return 1

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT symbol, status, L_price, U_price, ref_price_ws, atr, W_atr,
               mid_band_low, mid_band_high, mid_price
        FROM structural_cycle_symbols
        WHERE cycle_id = ?
        ORDER BY symbol
        """,
        (cid,),
    ).fetchall()
    conn.close()

    failures: list[str] = []

    for row in rows:
        sym = row["symbol"]
        stt = row["status"]
        if stt != "ok":
            continue
        lp, up, refp, atr = row["L_price"], row["U_price"], row["ref_price_ws"], row["atr"]
        if lp is None or up is None or refp is None or atr is None or float(atr) <= 0:
            failures.append(f"{sym}: ok row missing L/U/ref/atr")
            continue
        lp, up, refp, atr = float(lp), float(up), float(refp), float(atr)
        if not (lp < refp < up):
            failures.append(f"{sym}: expected L < ref < U, got L={lp} ref={refp} U={up}")
        w = (up - lp) / atr
        if row["W_atr"] is not None and abs(float(row["W_atr"]) - w) > 1e-6:
            failures.append(f"{sym}: W_atr mismatch stored={row['W_atr']} calc={w}")
        mid_geom = (lp + up) / 2.0
        if row["mid_price"] is not None:
            mid = float(row["mid_price"])
            if abs(mid - mid_geom) > 1e-6:
                failures.append(f"{sym}: mid != (L+U)/2")
        else:
            mid = mid_geom
        if row["mid_band_low"] is not None and row["mid_band_high"] is not None:
            lo, hi = float(row["mid_band_low"]), float(row["mid_band_high"])
            if not (lo <= mid <= hi):
                failures.append(f"{sym}: mid outside mid_band")
            if not (lo < hi):
                failures.append(f"{sym}: mid_band_low >= mid_band_high")

    if r.get("phase") == "armed":
        need = int(r.get("min_pool_required") or 0)
        got = int(r.get("symbols_ok") or 0)
        if got < need:
            failures.append(f"phase armed but symbols_ok {got} < min_pool {need}")
    elif r.get("phase") == "cancelled":
        print("NOTE: scan cancelled (often insufficient_pool) — инварианты ok-строк ниже только если есть.")

    if failures:
        print("FAIL:")
        for f in failures:
            print(" ", f)
        return 1

    print(f"OK: {len(rows)} symbol rows checked, {r.get('symbols_ok')} ok status")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
