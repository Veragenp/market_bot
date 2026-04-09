from __future__ import annotations

import json

from trading_bot.data.structural_cycle_db import run_structural_pipeline


def main() -> None:
    res = run_structural_pipeline()
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

