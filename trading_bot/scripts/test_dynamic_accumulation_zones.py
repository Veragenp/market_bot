"""
Тест выгрузки динамических зон накопления (BTC/USDT, 1m, календарный месяц) — исходное ТЗ.

Пример:
  python -m trading_bot.scripts.test_dynamic_accumulation_zones --year 2026 --month 3
  python -m trading_bot.scripts.test_dynamic_accumulation_zones --csv data/dynamic_zones_btc.csv
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from trading_bot.analytics.dynamic_accumulation_zones import (
    DEFAULT_CLUSTER_THRESHOLD_PCT,
    DEFAULT_POC_MERGE_THRESHOLD_PCT,
    DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT,
    run_pipeline,
)
from trading_bot.config.settings import DB_PATH


def load_btc_1m(
    conn: sqlite3.Connection,
    symbol: str = "BTC/USDT",
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    q = """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
    """
    params: list = [symbol]
    if start_ts is not None:
        q += " AND timestamp >= ?"
        params.append(start_ts)
    if end_ts is not None:
        q += " AND timestamp <= ?"
        params.append(end_ts)
    q += " ORDER BY timestamp"
    return pd.read_sql_query(q, conn, params=params)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Динамические зоны накопления: первичный скан по ТЗ; опции — расширенный конвейер"
    )
    p.add_argument("--symbol", default="BTC/USDT")
    p.add_argument("--year", type=int, help="Календарный год UTC (с --month)")
    p.add_argument("--month", type=int, help="Месяц 1–12 UTC")
    p.add_argument(
        "--csv",
        default="",
        help="Записать таблицу в CSV (UTF-8)",
    )
    p.add_argument(
        "--poc-threshold-pct",
        type=float,
        default=None,
        help=(
            "Порог слияния соседних часов: |POC_новее − POC_старше| ≤ доля × цена "
            f"(по ТЗ {DEFAULT_POC_MERGE_THRESHOLD_PCT} = {100 * DEFAULT_POC_MERGE_THRESHOLD_PCT:.2f}%)"
        ),
    )
    p.add_argument(
        "--bin-step",
        type=float,
        default=None,
        help="Фиксированный шаг биннинга USDT (по умолчанию: max(10, 0.02%% от последнего Close месяца))",
    )
    p.add_argument("--min-zone-hours", type=float, default=4.0)
    p.add_argument(
        "--rescan",
        action="store_true",
        help="Расширение: кластер уровней + Master POC (не из исходного ТЗ)",
    )
    p.add_argument(
        "--cluster-pct",
        type=float,
        default=None,
        help=f"При --rescan: порог кластера по цене, доля (default {DEFAULT_CLUSTER_THRESHOLD_PCT})",
    )
    p.add_argument(
        "--top-n-per-band",
        type=int,
        default=None,
        help="При --rescan: в каждой ценовой полосе не более N уровней по объёму (0 = все)",
    )
    p.add_argument(
        "--price-band-usdt",
        type=float,
        default=None,
        help="Ширина ценовой полосы USDT для top-N (default: 50 × tick_step)",
    )
    p.add_argument(
        "--no-weighted-merge",
        action="store_true",
        help="Отключить взвешенное слияние близких по цене зон после скана",
    )
    p.add_argument(
        "--weighted-merge-pct",
        type=float,
        default=None,
        help=(
            "Порог слияния соседних по цене зон: (p_high−p_low)/p_lower "
            f"(default {DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT} = 0.5%%); 0 — отключить"
        ),
    )
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    df = load_btc_1m(conn, symbol=args.symbol)
    conn.close()

    if df.empty:
        print("No 1m rows in DB for symbol. Load Binance 1m first.")
        return

    year, month = args.year, args.month
    if year is None or month is None:
        now = datetime.now(timezone.utc)
        m = now.month - 1
        y = now.year
        if m == 0:
            m = 12
            y -= 1
        year, month = y, m
        print(f"Using calendar month UTC (default): {year}-{month:02d}")

    rp_kw: dict = {"min_zone_hours": args.min_zone_hours, "rescan": args.rescan}
    if args.poc_threshold_pct is not None:
        rp_kw["poc_merge_threshold_pct"] = args.poc_threshold_pct
    if args.bin_step is not None:
        rp_kw["zone_bin_step_usdt"] = args.bin_step
    if args.cluster_pct is not None:
        rp_kw["cluster_threshold_pct"] = args.cluster_pct
    if args.top_n_per_band is not None:
        rp_kw["top_n_per_band"] = max(0, args.top_n_per_band)
    if args.price_band_usdt is not None:
        rp_kw["price_band_usdt"] = args.price_band_usdt
    if args.no_weighted_merge:
        rp_kw["weighted_merge_threshold_pct"] = None
    elif args.weighted_merge_pct is not None:
        rp_kw["weighted_merge_threshold_pct"] = (
            args.weighted_merge_pct if args.weighted_merge_pct > 0 else None
        )

    out, step = run_pipeline(df, year=year, month=month, **rp_kw)

    thr = args.poc_threshold_pct if args.poc_threshold_pct is not None else DEFAULT_POC_MERGE_THRESHOLD_PCT
    cl = args.cluster_pct if args.cluster_pct is not None else DEFAULT_CLUSTER_THRESHOLD_PCT
    if "weighted_merge_threshold_pct" not in rp_kw:
        wm_disp: float | None = DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT
    else:
        wm_disp = rp_kw["weighted_merge_threshold_pct"]
    wm_s = "off" if wm_disp is None else f"{wm_disp} ({100 * float(wm_disp):.3f}%)"
    print(
        f"tick_step: {step:g}; merge_threshold: {thr} ({100 * thr:.3f}%); "
        f"rescan: {args.rescan}; cluster_pct: {cl}; weighted_merge: {wm_s}"
    )
    print(f"zones found: {len(out)}")
    if out.empty:
        print("No zones (check month range vs available 1m data).")
        return

    disp = out.drop(columns=["t_start_unix", "t_end_unix"], errors="ignore")
    print(disp.to_string(index=False))

    if args.csv:
        out.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"Wrote {args.csv}")


if __name__ == "__main__":
    main()
