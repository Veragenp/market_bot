from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import PRO_LEVELS_LOOKBACK_DAYS, PRO_LEVELS_LOOKBACK_HOURS
from trading_bot.analytics.vp_ohlc_source import select_vp_ohlcv_dataframe
from trading_bot.analytics.volume_profile_peaks import (
    _profile_tick_size_eff,
    _volume_profile_from_hl,
    find_pro_levels,
    get_adaptive_params,
)
from trading_bot.data.db import get_connection
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import (
    _compute_find_pro_params,
    _fetch_ohlcv_1m_range,
    _get_last_ts_1m,
)

SYM = "NEAR/USDT"


def main() -> None:
    lb = int(PRO_LEVELS_LOOKBACK_DAYS or 0) * 86400 + int(PRO_LEVELS_LOOKBACK_HOURS or 0) * 3600
    end = _get_last_ts_1m(SYM)
    start = end - lb
    df = _fetch_ohlcv_1m_range(SYM, start, end)
    df = df.dropna(subset=["close", "volume"])
    for c in ("high", "low"):
        if c in df.columns:
            df[c] = df[c].fillna(df["close"])

    df_vp, tf, diag = select_vp_ohlcv_dataframe(df)
    cp = float(df_vp["close"].iloc[-1])
    ap = get_adaptive_params(df_vp, symbol=SYM)
    tick = _profile_tick_size_eff(float(ap["tick_size"]), cp)
    prof = _volume_profile_from_hl(df_vp[["low", "high", "volume"]], tick)
    win = 5
    sm = prof.rolling(win, center=True, min_periods=1).mean().fillna(0)
    top15 = sm.nlargest(15)

    print("NEAR tf=", tf, "vp_src=", diag.get("vp_source"))
    print("tick_eff", tick, "current_price", cp, "distance_pct", ap.get("distance_pct"))
    print("Top15 smoothed (bin -> vol_sm):")
    for p, v in top15.items():
        print(f"  {float(p):.6f}  {float(v):.2f}")

    common = _compute_find_pro_params(df_vp, SYM)
    out = find_pro_levels(df_vp, symbol=SYM, **common)
    print("find_pro_levels Price:", list(out["Price"].values) if not out.empty else [])

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT price, volume_peak, tier
        FROM price_levels
        WHERE symbol = ? AND level_type = 'vp_local' AND is_active = 1
        ORDER BY volume_peak DESC
        """,
        (SYM,),
    ).fetchall()
    print("SQLite active:", [tuple(r) for r in rows])
    conn.close()


if __name__ == "__main__":
    main()
