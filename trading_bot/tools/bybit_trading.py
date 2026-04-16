"""Bybit USDT linear: чтение баланса/позиций и размещение ордеров (demo или prod).

Ключи только из env: при BYBIT_USE_DEMO=1 — BYBIT_API_KEY_TEST / BYBIT_API_SECRET_TEST
и pybit HTTP(demo=True). Исполнение ордеров — только если BYBIT_EXECUTION_ENABLED=1.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

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
        # Bybit V5 position/list требует symbol ИЛИ settleCoin.
        params: Dict[str, Any] = {"category": "linear", "limit": 200}
        if symbol:
            params["symbol"] = to_bybit_symbol(symbol)
        else:
            params["settleCoin"] = "USDT"
        return sess.get_positions(**params)
    except Exception:
        logger.exception("get_positions failed")
        return None


def get_linear_open_orders(symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Открытые ордера linear (USDT settleCoin при symbol=None)."""
    try:
        sess = _session()
        params: Dict[str, Any] = {"category": "linear", "limit": 200}
        if symbol:
            params["symbol"] = to_bybit_symbol(symbol)
        else:
            params["settleCoin"] = "USDT"
        return sess.get_open_orders(**params)
    except Exception:
        logger.exception("get_open_orders failed")
        return None


def linear_position_sizes_by_symbol(resp: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """
    По ответу get_positions: символ Bybit (BTCUSDT) -> размер позиции (|size| может быть 0).
    Односторонний режим: одна строка на контракт.
    """
    out: Dict[str, float] = {}
    if not resp or not resp.get("result"):
        return out
    for row in (resp.get("result") or {}).get("list") or []:
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            sz = float(row.get("size") or 0.0)
        except (TypeError, ValueError):
            sz = 0.0
        out[sym] = sz
    return out


def pool_symbols_flat_on_linear_exchange(
    symbols_trade: List[str], sizes_by_bybit_symbol: Dict[str, float], *, eps: float = 1e-12
) -> bool:
    """True, если по всем символам пула на бирже нет открытого size (включая отсутствие ключа = 0)."""
    for s in symbols_trade:
        key = to_bybit_symbol(s)
        if abs(float(sizes_by_bybit_symbol.get(key, 0.0))) > eps:
            return False
    return True


def place_linear_limit_order(
    *,
    symbol_trade: str,
    side_buy: bool,
    qty: float,
    price: float,
    time_in_force: str = "GTC",
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Лимитный ордер USDT linear (вход).
    Как tutorial_v3/bybit_api.place_limit_order: опционально stopLoss / takeProfit на той же заявке (GTC).
    """
    from trading_bot.config import settings as st

    if not st.BYBIT_EXECUTION_ENABLED:
        raise RuntimeError("BYBIT_EXECUTION_ENABLED is off — refusing to place order")
    sess = _session()
    sym = to_bybit_symbol(symbol_trade)
    side = "Buy" if side_buy else "Sell"
    payload: Dict[str, Any] = {
        "category": "linear",
        "symbol": sym,
        "side": side,
        "orderType": "Limit",
        "qty": str(qty),
        "price": str(price),
        "timeInForce": time_in_force,
    }
    if stop_loss is not None:
        payload["stopLoss"] = str(stop_loss)
    if take_profit is not None:
        payload["takeProfit"] = str(take_profit)
    return sess.place_order(**payload)


def cancel_linear_order(*, symbol_trade: str, order_id: str) -> Dict[str, Any]:
    """Отмена ордера по orderId (linear). symbol_trade: BTC/USDT или BTCUSDT."""
    from trading_bot.config import settings as st

    if not st.BYBIT_EXECUTION_ENABLED:
        raise RuntimeError("BYBIT_EXECUTION_ENABLED is off — refusing to cancel order")
    sess = _session()
    sym = to_bybit_symbol(symbol_trade)
    return sess.cancel_order(category="linear", symbol=sym, orderId=str(order_id))


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
