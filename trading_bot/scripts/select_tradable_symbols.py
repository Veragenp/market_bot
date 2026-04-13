from __future__ import annotations

import argparse
import json
import os

from trading_bot.analytics.tradable_symbols_selector import (
    TradableSelectorParams,
    select_tradable_symbols,
)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    return int(raw) if raw else default


def _env_csv(name: str, default_csv: str) -> tuple[str, ...]:
    raw = (os.getenv(name, "") or "").strip() or default_csv
    return tuple(x.strip() for x in raw.split(",") if x.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Select tradable Bybit futures symbols")
    parser.add_argument("--top", type=int, default=_env_int("SYMBOL_SELECTOR_TOP_N", 30))
    parser.add_argument("--json", action="store_true", help="print full diagnostics JSON")
    args = parser.parse_args()

    p = TradableSelectorParams(
        volume_threshold=_env_float("SYMBOL_SELECTOR_VOLUME_THRESHOLD", 10_000_000.0),
        top_n=int(args.top),
        timeframe=os.getenv("SYMBOL_SELECTOR_TIMEFRAME", "15").strip() or "15",
        lookback_bars=_env_int("SYMBOL_SELECTOR_LOOKBACK_BARS", 672),
        velocity_window_bars=_env_int("SYMBOL_SELECTOR_VELOCITY_WINDOW_BARS", 96),
        benchmark_symbols=_env_csv("SYMBOL_SELECTOR_BENCHMARKS", "BTCUSDT,ETHUSDT"),
        weight_corr=_env_float("SYMBOL_SELECTOR_WEIGHT_CORR", 0.55),
        weight_velocity=_env_float("SYMBOL_SELECTOR_WEIGHT_VELOCITY", 0.05),
        weight_liquidity=_env_float("SYMBOL_SELECTOR_WEIGHT_LIQUIDITY", 0.40),
        exclude_bases=_env_csv(
            "SYMBOL_SELECTOR_EXCLUDE_BASES",
            "XAU,XAG,XAUT,PAXG,USDC,USDE,USD0,FDUSD,TUSD,USDP",
        ),
        exclude_numeric_bases=(os.getenv("SYMBOL_SELECTOR_EXCLUDE_NUMERIC", "1").strip() not in ("0", "false")),
        symbol_suffix=os.getenv("SYMBOL_SELECTOR_SUFFIX", "USDT").strip() or "USDT",
    )

    top, diag = select_tradable_symbols(p)
    if args.json:
        print(
            json.dumps(
                {
                    "params": {
                        "volume_threshold": p.volume_threshold,
                        "top_n": p.top_n,
                        "timeframe": p.timeframe,
                        "lookback_bars": p.lookback_bars,
                        "velocity_window_bars": p.velocity_window_bars,
                        "benchmarks": list(p.benchmark_symbols),
                        "weights": {
                            "corr": p.weight_corr,
                            "velocity": p.weight_velocity,
                            "liquidity": p.weight_liquidity,
                        },
                    },
                    "top_symbols": top,
                    "diagnostics": diag,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    print("=== Top symbols (Bybit format) ===")
    for s in top:
        print(s)
    print("\n=== Top symbols (trading_bot format) ===")
    for s in top:
        if s.endswith("USDT"):
            print(f"{s[:-4]}/USDT")
        else:
            print(s)


if __name__ == "__main__":
    main()

