from __future__ import annotations

"""
Совместимый интерфейс "db_client", который раньше жил в `src/provider/db_client.py`.

После удаления `src/` все импорты должны переходить на `trading_bot.data.db_client`.
"""

from trading_bot.data.repositories import (  # noqa: F401
    clean_old_minute_data,
    get_instrument,
    get_last_update,
    get_ohlcv,
    get_ohlcv_filled,
    save_instrument,
    save_liquidations,
    save_ohlcv,
    save_open_interest,
    set_last_cleaned,
    update_metadata,
)

