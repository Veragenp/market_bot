"""
Проверка демо-контура Bybit: баланс USDT и открытые linear-позиции.

  set BYBIT_USE_DEMO=1
  set BYBIT_API_KEY_TEST=...
  set BYBIT_API_SECRET_TEST=...

  PYTHONPATH=. python -m trading_bot.scripts.bybit_demo_status
"""

from __future__ import annotations

import json
import logging
import sys

_REPO = __import__("os").path.abspath(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.tools import bybit_trading as bt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> None:
    w = bt.get_wallet_usdt_balance()
    print("=== wallet (USDT) ===")
    print(bt.summarize_balance(w))
    if w:
        print(json.dumps(w, ensure_ascii=False, indent=2)[:4000])

    p = bt.get_linear_positions()
    print("=== positions linear ===")
    if p:
        print(json.dumps(p, ensure_ascii=False, indent=2)[:8000])
    else:
        print("(no response)")


if __name__ == "__main__":
    main()
