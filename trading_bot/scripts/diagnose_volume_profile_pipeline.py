"""
Диагностика VP: свежесть ohlcv 1m в SQLite и где отсекаются уровни (strict raw → dedup → full final).

Запуск из корня репозитория:
  python trading_bot/scripts/diagnose_volume_profile_pipeline.py
  python trading_bot/scripts/diagnose_volume_profile_pipeline.py --symbols BTC/USDT,WIF/USDT
"""

from __future__ import annotations

import argparse
import inspect
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_MARKET_BOT_ROOT = Path(__file__).resolve().parents[2]
if str(_MARKET_BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(_MARKET_BOT_ROOT))

from config import (  # noqa: E402
    PRO_LEVELS_LOOKBACK_DAYS,
    PRO_LEVELS_LOOKBACK_HOURS,
    TRADING_SYMBOLS,
)
from trading_bot.analytics.volume_profile_peaks import (  # noqa: E402
    find_pro_levels,
    get_adaptive_params,
    _ensure_ohlc_for_profile,
    _find_pro_levels_single_pass,
    _price_decimals_for_output,
    _profile_tick_size_eff,
    _volume_profile_from_hl,
)
from trading_bot.data.db import get_connection  # noqa: E402
from trading_bot.data.schema import init_db  # noqa: E402
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import (  # noqa: E402
    _compute_find_pro_params,
    _fetch_ohlcv_1m_range,
    _get_last_ts_1m,
)


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _lookback_seconds() -> int:
    d = int(PRO_LEVELS_LOOKBACK_DAYS or 0)
    h = int(PRO_LEVELS_LOOKBACK_HOURS or 0)
    return d * 86400 + h * 3600


def print_db_ohlcv_overview() -> None:
    init_db()
    conn = get_connection()
    row = conn.execute(
        "SELECT MAX(timestamp) AS mx FROM ohlcv WHERE timeframe = ?",
        ("1m",),
    ).fetchone()
    gmax = row["mx"] if row else None
    print("\n=== DB: global MAX(timestamp) ohlcv timeframe=1m ===")
    if gmax is None:
        print("  (no 1m rows)")
    else:
        print(f"  {int(gmax)}  {_iso(int(gmax))}")

    lb = _lookback_seconds()
    print(
        f"\n=== DB: VP window (same as rebuild) "
        f"(PRO_LEVELS_LOOKBACK_DAYS={PRO_LEVELS_LOOKBACK_DAYS}, "
        f"HOURS={PRO_LEVELS_LOOKBACK_HOURS} -> {lb}s) ==="
    )
    print("  Anchor = last 1m bar per symbol; window [end_ts - lookback, end_ts].\n")

    for sym in TRADING_SYMBOLS:
        end_ts = _get_last_ts_1m(sym)
        if end_ts is None:
            print(f"  {sym}: no 1m data")
            continue
        start_ts = end_ts - lb
        cnt = conn.execute(
            """
            SELECT COUNT(*) AS c FROM ohlcv
            WHERE symbol = ? AND timeframe = '1m' AND timestamp >= ? AND timestamp <= ?
            """,
            (sym, start_ts, end_ts),
        ).fetchone()["c"]
        print(
            f"  {sym}: bars={cnt}  last={_iso(end_ts)}  "
            f"window_start={_iso(start_ts)}  window_end={_iso(end_ts)}"
        )
    conn.close()


def _prepare_work(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work = work.dropna(subset=["close", "volume"])
    work = work[work["volume"] >= 0]
    work = _ensure_ohlc_for_profile(work)
    return work.dropna(subset=["high", "low"])


def _kwargs_for_find_pro_levels(symbol: str, common: dict) -> dict:
    """Те же дефолты, что у find_pro_levels, перекрытые common — как при rebuild."""
    sig = inspect.signature(find_pro_levels)
    out: dict = {}
    for name, p in sig.parameters.items():
        if name == "df":
            continue
        if p.default is not inspect.Parameter.empty:
            out[name] = p.default
    out.update(common)
    out["symbol"] = symbol
    return out


def _strict_pass_stage_counts(work: pd.DataFrame, merged_kw: dict) -> tuple[int, int, int]:
    """Жёсткий проход: после greedy+tiers → после dedup → после финального merge цен в проходе."""
    cp = float(work["close"].iloc[-1])
    sym = (merged_kw.get("symbol") or "")
    sym = str(sym).strip() or None
    tick_size_eff = _profile_tick_size_eff(merged_kw.get("tick_size"), cp)
    price_decimals = int(_price_decimals_for_output(sym))
    profile = _volume_profile_from_hl(work[["low", "high", "volume"]], tick_size_eff)
    win = max(3, int(merged_kw.get("smoothing_window", 5)))
    if win % 2 == 0:
        win += 1
    volume_sm = profile.rolling(window=win, center=True, min_periods=1).mean().fillna(0.0)

    strict_weak_pct = (
        float(merged_kw["strict_height_percentile_weak"])
        if merged_kw.get("strict_height_percentile_weak") is not None
        else float(merged_kw["height_percentile_weak"])
    )
    strict_hm = (
        merged_kw["strict_height_mult"]
        if merged_kw.get("strict_height_mult") is not None
        else merged_kw.get("height_mult")
    )

    raw_s, dedup_s, strict_full = _find_pro_levels_single_pass(
        work,
        volume_sm,
        tick_size_eff=tick_size_eff,
        current_price=cp,
        height_percentile=float(merged_kw["height_percentile"]),
        height_percentile_strong=float(merged_kw["height_percentile_strong"]),
        height_percentile_weak=float(strict_weak_pct),
        height_mult=strict_hm,
        distance_pct=float(merged_kw["distance_pct"]),
        merge_distance_pct=float(merged_kw["merge_distance_pct"]),
        valley_threshold=float(merged_kw["valley_threshold"]),
        valley_merge_threshold=merged_kw.get("valley_merge_threshold"),
        enable_valley_merge=bool(merged_kw["enable_valley_merge"]),
        min_duration_hours=float(merged_kw["min_duration_hours"]),
        top_n=int(merged_kw["top_n"]),
        max_levels=merged_kw.get("max_levels"),
        include_all_tiers=bool(merged_kw["include_all_tiers"]),
        allow_stage_b_overlap=bool(merged_kw["allow_stage_b_overlap"]),
        include_weak=True,
        duration_thresholds=merged_kw.get("duration_thresholds"),
        dedup_round_pct=float(merged_kw["dedup_round_pct"]),
        final_merge_pct=merged_kw.get("final_merge_pct"),
        final_merge_valley_threshold=merged_kw.get("final_merge_valley_threshold"),
        reserved_prices=None,
        exclude_reserved_pct=None,
        skip_final_merge_clamp=False,
        price_decimals=price_decimals,
    )
    return len(raw_s), len(dedup_s), len(strict_full)


def print_symbol_pipeline(symbol: str) -> None:
    lb = _lookback_seconds()
    end_ts = _get_last_ts_1m(symbol)
    if end_ts is None:
        print(f"\n--- {symbol}: SKIP (no anchor ts) ---")
        return
    start_ts = end_ts - lb
    df = _fetch_ohlcv_1m_range(symbol, start_ts, end_ts)
    if df.empty:
        print(f"\n--- {symbol}: SKIP (empty window) ---")
        return

    work = _prepare_work(df)
    print(f"\n--- {symbol} ---")
    print(f"  bars_in_df={len(df)}  bars_after_hl_clean={len(work)}")
    if work.empty:
        return

    ap = get_adaptive_params(work, symbol=symbol)
    keys = (
        "daily_vol_pct",
        "typical_range",
        "volume_cv",
        "tick_size",
        "distance_pct",
        "merge_distance_pct",
        "dynamic_merge_pct",
        "min_duration_hours",
        "top_n",
        "height_mult",
        "height_percentile",
        "height_percentile_strong",
        "height_percentile_weak",
        "valley_threshold",
        "valley_merge_threshold",
    )
    print("  get_adaptive_params (raw; rebuild may override via PRO_LEVELS_*):")
    for k in keys:
        if k in ap:
            print(f"    {k}={ap[k]}")

    common = _compute_find_pro_params(work, symbol)
    merged = _kwargs_for_find_pro_levels(symbol, common)
    n_pre_dedup, n_post_dedup, n_strict_merged = _strict_pass_stage_counts(work, merged)

    cp = float(work["close"].iloc[-1])
    tick_eff = _profile_tick_size_eff(common.get("tick_size"), cp)
    profile = _volume_profile_from_hl(work[["low", "high", "volume"]], tick_eff)
    win = 5
    vol_sm = profile.rolling(window=win, center=True, min_periods=1).mean().fillna(0.0)
    top = vol_sm.nlargest(5)
    print(f"  profile_bins={len(profile)}  smoothed_top5 (bin_center -> vol_sm):")
    for price, v in top.items():
        print(f"    {float(price):.8g}  ->  {float(v):.4g}")

    print("  find_pro_levels (two_pass, same as rebuild), strict-pass stages:")
    print(
        f"    strict_after_greedy_tiers_pre_dedup={n_pre_dedup}  "
        f"strict_after_dedup_pre_merge={n_post_dedup}  "
        f"strict_after_final_merge_in_pass={n_strict_merged}"
    )
    raw_df = find_pro_levels(work, symbol=symbol, **common, return_raw=True)
    ded_df = find_pro_levels(work, symbol=symbol, **common, return_dedup=True)
    fin_df = find_pro_levels(work, symbol=symbol, **common)
    print(
        f"    (cross_check return_raw={len(raw_df)} return_dedup={len(ded_df)})  "
        f"FINAL_after_soft+combined_merge={len(fin_df)}"
    )
    if not fin_df.empty:
        print("    FINAL (Price, Volume, Tier):")
        for _, r in fin_df.head(15).iterrows():
            print(f"      {float(r['Price']):.8g}  vol={float(r['Volume']):.2f}  {r.get('Tier', '')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="VP pipeline + ohlcv freshness")
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Через запятую, например BTC/USDT,WIF/USDT; пусто = все TRADING_SYMBOLS",
    )
    args = parser.parse_args()

    print_db_ohlcv_overview()

    if args.symbols.strip():
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        syms = list(TRADING_SYMBOLS)

    print("\n=== Stages: strict greedy -> dedup -> strict merge -> soft -> final merge ===")
    for sym in syms:
        print_symbol_pipeline(sym)


if __name__ == "__main__":
    main()
