"""
Создать черновик позиции из строки `entry_gate_confirmations`.

Примеры:
  python -m trading_bot.scripts.open_position_from_confirmation --id 1
  python -m trading_bot.scripts.open_position_from_confirmation --id 1 --limit
"""

from __future__ import annotations

import argparse
import json
import sys

from trading_bot.data.db import get_connection
from trading_bot.data.position_opening import create_draft_position_from_confirmation
from trading_bot.data.schema import run_migrations


def main() -> int:
    p = argparse.ArgumentParser(description="Черновик position_records из entry_gate_confirmations")
    p.add_argument("--id", type=int, required=True, help="id строки entry_gate_confirmations")
    p.add_argument(
        "--limit",
        action="store_true",
        help="Лимит GTC по плану со встроенным stopLoss (нужны BYBIT_EXECUTION_ENABLED=1 и ключи)",
    )
    args = p.parse_args()

    run_migrations()
    conn = get_connection()
    try:
        cur = conn.cursor()
        out = create_draft_position_from_confirmation(
            cur,
            confirmation_id=args.id,
            execute_limit=args.limit,
        )
        if out.get("ok"):
            conn.commit()
        else:
            conn.rollback()
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
        return 0 if out.get("ok") else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
