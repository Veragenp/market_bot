"""Общие параметры pybit `unified_trading.WebSocket` для публичного linear V5."""

from __future__ import annotations

import logging
from typing import Any, Dict

from trading_bot.config import settings as st

logger = logging.getLogger(__name__)

_BYTICK_SUPPRESSED_FOR_DEMO_LOGGED = False


def public_linear_websocket_kwargs() -> Dict[str, Any]:
    """
    Аргументы для `WebSocket(channel_type=\"linear\", **kwargs)`.
    `channel_type` передаётся отдельно у вызывающего кода.

    `domain`: пусто → pybit по умолчанию (bybit); `bytick` — зеркало (регионы / 404 от ELB),
    **только при BYBIT_USE_DEMO=0**. В демо зеркало для публичного WS не используем — только
    `stream-demo.bybit.com`, иначе подключение часто падает.
    """
    kw: Dict[str, Any] = {
        "testnet": False,
        "demo": bool(st.BYBIT_USE_DEMO),
        "ping_interval": int(st.BYBIT_WS_PING_INTERVAL),
        "ping_timeout": int(st.BYBIT_WS_PING_TIMEOUT),
        "retries": int(st.BYBIT_WS_RETRIES),
        "trace_logging": bool(st.BYBIT_WS_TRACE_LOGGING),
    }
    dom = (st.BYBIT_WS_DOMAIN or "").strip().lower()
    global _BYTICK_SUPPRESSED_FOR_DEMO_LOGGED
    if st.BYBIT_USE_DEMO and dom == "bytick":
        if not _BYTICK_SUPPRESSED_FOR_DEMO_LOGGED:
            logger.info(
                "BYBIT_WS_DOMAIN=bytick ignored while BYBIT_USE_DEMO=1 — demo public WS uses "
                "stream-demo.bybit.com only."
            )
            _BYTICK_SUPPRESSED_FOR_DEMO_LOGGED = True
        dom = ""
    if dom in ("bytick", "bybit"):
        kw["domain"] = dom
    elif dom:
        logger.warning(
            "BYBIT_WS_DOMAIN=%r is not supported; use bytick, bybit, or leave empty. Ignored.",
            st.BYBIT_WS_DOMAIN,
        )
    return kw
