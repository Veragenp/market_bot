"""Remove all ohlcv + metadata rows for CoinGecko sources."""

from __future__ import annotations

import os
import sqlite3
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from trading_bot.config.settings import DB_PATH

SOURCES = ("coingecko", "coingecko_agg")


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for src in SOURCES:
        n = c.execute("SELECT COUNT(*) FROM ohlcv WHERE source=?", (src,)).fetchone()[0]
        print(f"ohlcv before {src}: {n}")
    c.execute(f"DELETE FROM ohlcv WHERE source IN ({','.join('?' * len(SOURCES))})", SOURCES)
    print(f"ohlcv deleted: {c.rowcount}")
    for src in SOURCES:
        n = c.execute("SELECT COUNT(*) FROM metadata WHERE source=?", (src,)).fetchone()[0]
        print(f"metadata before {src}: {n}")
    c.execute(f"DELETE FROM metadata WHERE source IN ({','.join('?' * len(SOURCES))})", SOURCES)
    print(f"metadata deleted: {c.rowcount}")
    conn.commit()
    conn.close()
    print("OK", DB_PATH)


if __name__ == "__main__":
    main()
