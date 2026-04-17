"""
Дополнительные функции supervisor для TEST_MODE оптимизации.
Включать только при TEST_MODE=1 в .env
"""

from __future__ import annotations

import logging
from typing import Dict

from trading_bot.config import settings as st

logger = logging.getLogger(__name__)


def get_test_mode_skipped_data_refresh_result() -> Dict[str, object]:
    """
    Возвращает результат для пропущенного DATA_REFRESH в тестовом режиме.
    """
    return {
        "steps": {
            "spot_main": "skipped",
            "spot_crypto_context": "skipped",
            "macro": "skipped",
            "indices_tv": "skipped",
            "oi_bybit": "skipped",
            "instruments": "skipped",
            "instruments_atr": "skipped",
        },
        "test_mode_optimization": True,
        "message": "All steps skipped - using cached ATR and prices from DB"
    }


def get_test_mode_skipped_levels_rebuild_result() -> Dict[str, int]:
    """
    Возвращает результат для пропущенного LEVELS_REBUILD в тестовом режиме.
    """
    return {"vp_local_rebuild": 0, "test_mode_optimization": True}


def format_test_mode_info() -> str:
    """
    Форматирует информацию о TEST_MODE для логирования.
    """
    if not st.TEST_MODE:
        return ""
    
    return (
        f" TEST_MODE=1 "
        f"skip_data={int(st.TEST_MODE_SKIP_DATA_REFRESH)} "
        f"skip_levels={int(st.TEST_MODE_SKIP_LEVELS_REBUILD)} "
        f"skip_vp_export={int(st.TEST_MODE_SKIP_VP_EXPORT)}"
    )
