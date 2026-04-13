"""Bybit USDT linear: чтение баланса/позиций и размещение ордеров (demo или prod).

Ключи только из env: при BYBIT_USE_DEMO=1 — BYBIT_API_KEY_TEST / BYBIT_API_SECRET_TEST
и pybit HTTP(demo=True). Исполнение ордеров — только если BYBIT_EXECUTION_ENABLED=1.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _session():
    from pybit.unified_trading import HTTP  # type: ignore

    from trading_bot.config import settings as st

    if st.BYBIT_USE_DEMO:
        k, s = st.BYBIT_API_KEY_TEST, st.BYBIT_API_SECRET_TEST
        if not k or not s:
            raise RuntimeError("BYBIT_USE_DEMO=1 but BYBIT_API_KEY_TEST / BYBIT_API_SECRET_TEST are empty")
        return HTTP(demo=True, api_key=k, api_secret=s)
    k, s = st.BYBIT_API_KEY, st.BYBIT_API_SECRET
    if not k or not s:
        raise RuntimeError("Bybit API keys missing (set BYBIT_API_KEY / BYBIT_API_SECRET or demo keys)")
    return HTTP(testnet=False, api_key=k, api_secret=s)


def to_bybit_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()


def get_wallet_usdt_balance() -> Optional[Dict[str, Any]]:
    """Unified trading account USDT (coin), если доступно."""
    try:
        sess = _session()
        r = sess.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        return r
    except Exception:
        logger.exception("get_wallet_balance failed")
        return None


def get_linear_positions(symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    try:
        sess = _session()
        params: Dict[str, Any] = {"category": "linear", "limit": 50}
        if symbol:
            params["symbol"] = to_bybit_symbol(symbol)
        return sess.get_positions(**params)
    except Exception:
        logger.exception("get_positions failed")
        return None


def place_linear_limit_order(
    *,
    symbol_trade: str,
    side_buy: bool,
    qty: float,
    price: float,
    time_in_force: str = "GTC",
) -> Dict[str, Any]:
    """Лимитный ордер USDT linear (вход)."""
    from trading_bot.config import settings as st

    if not st.BYBIT_EXECUTION_ENABLED:
        raise RuntimeError("BYBIT_EXECUTION_ENABLED is off — refusing to place order")
    sess = _session()
    sym = to_bybit_symbol(symbol_trade)
    side = "Buy" if side_buy else "Sell"
    return sess.place_order(
        category="linear",
        symbol=sym,
        side=side,
        orderType="Limit",
        qty=str(qty),
        price=str(price),
        timeInForce=time_in_force,
    )


def place_linear_market_order(
    *,
    symbol_trade: str,
    side_buy: bool,
    qty: float,
    reduce_only: bool = False,
) -> Dict[str, Any]:
    from trading_bot.config import settings as st

    if not st.BYBIT_EXECUTION_ENABLED:
        raise RuntimeError("BYBIT_EXECUTION_ENABLED is off — refusing to place order")
    sess = _session()
    sym = to_bybit_symbol(symbol_trade)
    side = "Buy" if side_buy else "Sell"
    return sess.place_order(
        category="linear",
        symbol=sym,
        side=side,
        orderType="Market",
        qty=str(qty),
        reduceOnly=reduce_only,
    )


def place_linear_stop_market_order(
    *,
    symbol_trade: str,
    side_buy: bool,
    qty: float,
    trigger_price: float,
    reduce_only: bool = True,
    close_on_trigger: bool = True,
) -> Dict[str, Any]:
    """
    Stop-Market для защиты позиции (SL). Для long-позиции side_buy=False (Sell), для short — Buy.
    """
    from trading_bot.config import settings as st

    if not st.BYBIT_EXECUTION_ENABLED:
        raise RuntimeError("BYBIT_EXECUTION_ENABLED is off — refusing to place stop order")
    sess = _session()
    sym = to_bybit_symbol(symbol_trade)
    side = "Buy" if side_buy else "Sell"
    # Bybit triggerDirection:
    #   1 => trigger when market price rises to triggerPrice
    #   2 => trigger when market price falls to triggerPrice
    # For Sell-stop below market (long SL) use 2; for Buy-stop above market (short SL) use 1.
    trigger_direction = 1 if side_buy else 2
    return sess.place_order(
        category="linear",
        symbol=sym,
        side=side,
        orderType="Market",
        qty=str(qty),
        triggerPrice=str(trigger_price),
        triggerDirection=trigger_direction,
        reduceOnly=reduce_only,
        closeOnTrigger=close_on_trigger,
    )


def build_client_order_id(prefix: str = "mb") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def summarize_balance(resp: Optional[Dict[str, Any]]) -> str:
    if not resp or not resp.get("result"):
        return "no data"
    try:
        lst = (resp.get("result") or {}).get("list") or []
        if not lst:
            return "empty list"
        coins = lst[0].get("coin") or []
        for c in coins:
            if str(c.get("coin") or "").upper() == "USDT":
                return json.dumps(
                    {
                        "walletBalance": c.get("walletBalance"),
                        "availableToWithdraw": c.get("availableToWithdraw"),
                        "equity": c.get("equity"),
                    },
                    ensure_ascii=False,
                )
        return json.dumps(lst[0], ensure_ascii=False)[:500]
    except Exception:
        return str(resp)[:500]
