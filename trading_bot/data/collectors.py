from __future__ import annotations

import logging
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from config import (
    ANALYTIC_SYMBOLS,
    DEFAULT_SOURCE_BINANCE,
    DEFAULT_SOURCE_YFINANCE,
    MINUTE_DATA_RETENTION_DAYS,
    SOURCE_BINANCE,
    SOURCE_COINGECKO,
    SOURCE_COINGECKO_AGG,
    SOURCE_YFINANCE,
    TIMEFRAMES_BY_CATEGORY,
    TRADING_SYMBOLS,
)
from trading_bot.data import db_client
from trading_bot.provider.exchange_factory import get_exchange_client

logger = logging.getLogger(__name__)

_HISTORY_START_TS = 1483228800  # 2017-01-01 00:00:00 UTC
_PAGE_LIMIT = 1000
_SYMBOL_SLEEP_SEC = 0.2


def _normalize_timeframe(timeframe: str) -> str:
    if timeframe == "1W":
        return "1w"
    return timeframe


def _is_minute_timeframe(timeframe: str) -> bool:
    return _normalize_timeframe(timeframe) == "1m"


def _determine_initial_since(symbol: str, timeframe: str) -> int:
    if _is_minute_timeframe(timeframe):
        return int(time.time()) - MINUTE_DATA_RETENTION_DAYS * 86400

    client = get_exchange_client(SOURCE_BINANCE)
    since = _HISTORY_START_TS
    if hasattr(client, "get_symbol_listing_ts"):
        listing_ts = client.get_symbol_listing_ts(symbol)  # type: ignore[attr-defined]
        if listing_ts:
            since = max(since, int(listing_ts))
    return since


def _fetch_ohlcv_incremental(
    symbol: str,
    timeframe: str,
    since: int,
    until: int,
) -> List[Dict[str, Any]]:
    client = get_exchange_client(SOURCE_BINANCE)
    all_records: List[Dict[str, Any]] = []
    cursor = since
    tf = _normalize_timeframe(timeframe)

    while cursor <= until:
        batch = client.fetch_ohlcv(symbol=symbol, timeframe=tf, since=cursor, limit=_PAGE_LIMIT)
        if not batch:
            break

        # Defensive dedupe against exchanges that can return overlapping edges.
        filtered = [r for r in batch if int(r["timestamp"]) >= cursor]
        if not filtered:
            break

        all_records.extend(filtered)
        last_ts = int(filtered[-1]["timestamp"])
        next_cursor = last_ts + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        if len(batch) < _PAGE_LIMIT:
            break

        time.sleep(_SYMBOL_SLEEP_SEC)

    # Ensure sorted unique timestamps before save.
    unique: Dict[int, Dict[str, Any]] = {}
    for rec in all_records:
        unique[int(rec["timestamp"])] = rec
    return [unique[ts] for ts in sorted(unique.keys())]


def update_binance_ohlcv(symbol: str, timeframe: str, days_back: int = None) -> None:
    """
    Обновляет OHLCV для одного символа и таймфрейма.

    Если данных нет:
      - для 1m: последние MINUTE_DATA_RETENTION_DAYS дней
      - иначе: с 2017-01-01 UTC (или позже, если найдена дата листинга).
    Если данные есть: загрузка только новых свечей с last_updated + 1.
    """
    tf = _normalize_timeframe(timeframe)
    now_ts = int(time.time())
    last_ts = db_client.get_last_update(symbol, tf, source=DEFAULT_SOURCE_BINANCE)

    if last_ts is None:
        since = _determine_initial_since(symbol, tf)
        if days_back is not None and days_back > 0:
            since = max(since, now_ts - days_back * 86400)
        is_full_load = True
    else:
        since = int(last_ts) + 1
        if days_back is not None and days_back > 0:
            since = max(since, now_ts - days_back * 86400)
        is_full_load = False

    if since >= now_ts:
        logger.info("No new range for %s %s (since=%s)", symbol, tf, since)
        return

    try:
        records = _fetch_ohlcv_incremental(
            symbol=symbol,
            timeframe=tf,
            since=since,
            until=now_ts,
        )
    except Exception as exc:
        logger.exception("Failed to update %s %s: %s", symbol, tf, exc)
        return

    if not records:
        logger.info("No new candles for %s %s", symbol, tf)
        return

    db_client.save_ohlcv(symbol, tf, records)
    max_ts = max(int(r["timestamp"]) for r in records)
    if is_full_load:
        db_client.update_metadata(symbol, tf, max_ts, max_ts, source=DEFAULT_SOURCE_BINANCE)
    else:
        db_client.update_metadata(symbol, tf, max_ts, source=DEFAULT_SOURCE_BINANCE)

    logger.info(
        "Updated %s %s: %s candles, range %s -> %s",
        symbol,
        tf,
        len(records),
        datetime.fromtimestamp(int(records[0]["timestamp"]), tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(int(records[-1]["timestamp"]), tz=timezone.utc).isoformat(),
    )


def update_yfinance_ohlcv(symbol: str, timeframe: str, days_back: int = None) -> None:
    """
    Обновляет macro OHLCV через yfinance.

    Если данных нет: грузим с 2017-01-01 UTC (или ограничиваем days_back).
    Если данные есть: грузим только новые свечи с last_updated + 1.
    """
    tf = _normalize_timeframe(timeframe)
    now_ts = int(time.time())
    last_ts = db_client.get_last_update(symbol, tf, source=DEFAULT_SOURCE_YFINANCE)

    if last_ts is None:
        since = _HISTORY_START_TS
        if days_back is not None and days_back > 0:
            since = max(since, now_ts - days_back * 86400)
        is_full_load = True
    else:
        since = int(last_ts) + 1
        if days_back is not None and days_back > 0:
            since = max(since, now_ts - days_back * 86400)
        is_full_load = False

    if since >= now_ts:
        logger.info("No new yfinance range for %s %s (since=%s)", symbol, tf, since)
        return

    try:
        if tf == "4h":
            from trading_bot.data.yahoo_finance_loader import YahooFinanceDataLoader

            records = YahooFinanceDataLoader().fetch_ohlcv(
                symbol=symbol,
                timeframe=tf,
                start_ts=since,
                end_ts=now_ts,
            )
        else:
            client = get_exchange_client(SOURCE_YFINANCE)
            records = client.fetch_ohlcv(
                symbol=symbol,
                timeframe=tf,
                start=since,
                end=now_ts,
            )
        if tf == "1w" and not records:
            client = get_exchange_client(SOURCE_YFINANCE)
            daily_records = client.fetch_ohlcv(
                symbol=symbol,
                timeframe="1d",
                start=since,
                end=now_ts,
            )
            records = _build_weekly_from_daily(daily_records)
    except Exception as exc:
        logger.exception("Failed to update yfinance %s %s: %s", symbol, tf, exc)
        return

    if not records:
        logger.info("No new yfinance candles for %s %s", symbol, tf)
        return

    db_client.save_ohlcv(symbol, tf, records)
    max_ts = max(int(r["timestamp"]) for r in records)
    if is_full_load:
        db_client.update_metadata(symbol, tf, max_ts, max_ts, source=DEFAULT_SOURCE_YFINANCE)
    else:
        db_client.update_metadata(symbol, tf, max_ts, source=DEFAULT_SOURCE_YFINANCE)

    logger.info(
        "Updated yfinance %s %s: %s candles, range %s -> %s",
        symbol,
        tf,
        len(records),
        datetime.fromtimestamp(int(records[0]["timestamp"]), tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(int(records[-1]["timestamp"]), tz=timezone.utc).isoformat(),
    )


def _build_weekly_from_daily(daily_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build 1w OHLCV candles from 1d candles (Monday 00:00 UTC anchor)."""
    if not daily_records:
        return []

    df = pd.DataFrame(daily_records)
    if df.empty:
        return []

    df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.sort_values("dt")
    df["week_start"] = (df["dt"].dt.normalize() - pd.to_timedelta(df["dt"].dt.weekday, unit="D"))

    result: List[Dict[str, Any]] = []
    for week_start, grp in df.groupby("week_start", sort=True):
        grp = grp.sort_values("dt")
        opens = grp["open"].dropna()
        highs = grp["high"].dropna()
        lows = grp["low"].dropna()
        closes = grp["close"].dropna()
        volumes = grp["volume"].dropna()
        if closes.empty:
            continue

        result.append(
            {
                "timestamp": int(pd.Timestamp(week_start).timestamp()),
                "open": float(opens.iloc[0]) if not opens.empty else None,
                "high": float(highs.max()) if not highs.empty else None,
                "low": float(lows.min()) if not lows.empty else None,
                "close": float(closes.iloc[-1]),
                "volume": float(volumes.sum()) if not volumes.empty else 0.0,
                "source": "yfinance",
            }
        )
    return result


def update_yfinance_macro_all(timeframes: Optional[List[str]] = None, days_back: int = None) -> None:
    """Обновляет все macro-символы из конфига через yfinance."""
    symbols = ANALYTIC_SYMBOLS.get("macro", [])
    target_timeframes = timeframes or ["1d", "1w", "1M"]
    for symbol in symbols:
        for timeframe in target_timeframes:
            update_yfinance_ohlcv(symbol=symbol, timeframe=timeframe, days_back=days_back)
            time.sleep(_SYMBOL_SLEEP_SEC)


def _meta_symbol_key(data_type: str, symbol: str) -> str:
    return f"{data_type}:{symbol}"


def update_indices(days_back: int = None) -> None:
    """
    Индексы TOTAL, TOTAL2, TOTAL3, BTCD, OTHERSD — OHLCV с TradingView (CRYPTOCAP).
    """
    from trading_bot.data.tradingview_loader import TradingViewDataLoader

    loader = TradingViewDataLoader()
    src = loader.get_exchange_name()
    now_ts = int(time.time())
    symbols = ANALYTIC_SYMBOLS.get("indices", [])
    timeframes = TIMEFRAMES_BY_CATEGORY.get("indices", ["1d", "1w", "1M"])

    for symbol in symbols:
        for timeframe in timeframes:
            tf = _normalize_timeframe(timeframe)
            last_ts = db_client.get_last_update(symbol, tf, source=src)
            if last_ts is None:
                since = _HISTORY_START_TS
                if tf == "1m":
                    since = max(since, now_ts - MINUTE_DATA_RETENTION_DAYS * 86400)
                if days_back is not None and days_back > 0:
                    since = max(since, now_ts - days_back * 86400)
                is_full_load = True
            else:
                since = int(last_ts) + 1
                if tf == "1m":
                    since = max(since, now_ts - MINUTE_DATA_RETENTION_DAYS * 86400)
                if days_back is not None and days_back > 0:
                    since = max(since, now_ts - days_back * 86400)
                is_full_load = False

            if since >= now_ts:
                continue

            try:
                records = loader.fetch_ohlcv(symbol=symbol, timeframe=tf, start_ts=since, end_ts=now_ts)
            except Exception as exc:
                logger.exception("Failed to update TradingView index %s %s: %s", symbol, tf, exc)
                continue

            if not records:
                logger.info("No TradingView rows for %s %s", symbol, tf)
                continue

            db_client.save_ohlcv(symbol, tf, records)
            max_ts = max(int(r["timestamp"]) for r in records)
            if is_full_load:
                db_client.update_metadata(symbol, tf, max_ts, max_ts, source=src)
            else:
                db_client.update_metadata(symbol, tf, max_ts, source=src)
            logger.info("Updated TradingView %s %s: %s rows", symbol, tf, len(records))
            time.sleep(_SYMBOL_SLEEP_SEC)


def update_aggregated_indices(limit: int = 500, timeframe: str = "1h") -> None:
    """
    Build TOTAL/TOTAL2/TOTAL3/BTCD from CoinGecko top market caps and store into ohlcv.
    """
    tf = _normalize_timeframe(timeframe)
    if tf not in {"1m", "1h", "1d"}:
        raise ValueError("Aggregated indices timeframe must be '1m', '1h' or '1d'.")

    client = get_exchange_client(SOURCE_COINGECKO)
    if hasattr(client, "delay_seconds"):
        try:
            client.delay_seconds = 0
        except Exception:
            pass
    try:
        df = client.fetch_top_market_cap(limit=limit)
    except Exception as exc:
        logger.exception("Failed to fetch top market cap data: %s", exc)
        return

    if df.empty:
        logger.warning("CoinGecko top market cap dataset is empty.")
        return

    total = float(df["market_cap"].sum())
    btc_cap = float(df.loc[df["symbol"] == "BTC", "market_cap"].sum())
    eth_cap = float(df.loc[df["symbol"] == "ETH", "market_cap"].sum())
    total2 = total - btc_cap
    total3 = total - btc_cap - eth_cap
    btcd = (btc_cap / total * 100.0) if total > 0 else 0.0
    others = total - btc_cap
    othersd = (others / total * 100.0) if total > 0 else 0.0
    total_volume = float(df["total_volume"].sum())

    now_ts = int(time.time())
    tf_seconds = 60 if tf == "1m" else (3600 if tf == "1h" else 86400)
    bucket_ts = (now_ts // tf_seconds) * tf_seconds

    values = {
        "TOTAL": total,
        "TOTAL2": total2,
        "TOTAL3": total3,
        "BTCD": btcd,
        "OTHERS": others,
        "OTHERSD": othersd,
    }
    extra = json.dumps(
        {
            "market_cap_total_top_n": total,
            "market_cap_btc": btc_cap,
            "market_cap_eth": eth_cap,
            "source_limit": int(limit),
        },
        ensure_ascii=True,
    )

    for symbol, value in values.items():
        rec = {
            "timestamp": bucket_ts,
            "open": value,
            "high": value,
            "low": value,
            "close": value,
            "volume": total_volume if symbol not in {"BTCD", "OTHERSD"} else 0.0,
            "source": SOURCE_COINGECKO_AGG,
            "extra": extra,
        }
        db_client.save_ohlcv(symbol, tf, [rec])
        last = db_client.get_last_update(symbol, tf, source=SOURCE_COINGECKO_AGG)
        if last is None:
            db_client.update_metadata(symbol, tf, bucket_ts, bucket_ts, source=SOURCE_COINGECKO_AGG)
        else:
            db_client.update_metadata(symbol, tf, bucket_ts, source=SOURCE_COINGECKO_AGG)


def backfill_aggregated_indices_daily_weekly(start_ts: int = _HISTORY_START_TS, end_ts: int = None) -> None:
    """
    Backfill computed indices (coingecko_agg) for 1d and 1w from CoinGecko global history.
    """
    client = get_exchange_client(SOURCE_COINGECKO)
    end_ts = int(time.time()) if end_ts is None else int(end_ts)
    start_ts = int(start_ts)
    if start_ts > end_ts:
        return

    metrics = client.fetch_global_range(start=start_ts, end=end_ts)
    if not metrics:
        logger.warning("No CoinGecko global history rows for backfill.")
        return

    daily_by_symbol: Dict[str, List[Dict[str, Any]]] = {
        "TOTAL": [],
        "TOTAL2": [],
        "TOTAL3": [],
        "BTCD": [],
        "OTHERS": [],
        "OTHERSD": [],
    }

    for m in metrics:
        total = float(m.get("total_market_cap") or 0.0)
        btcd = float(m.get("btcd") or 0.0)
        btc_cap = total * btcd / 100.0 if total > 0 else 0.0
        # CoinGecko global history usually has ETH share in percentage payload.
        ethd = float((m.get("market_cap_percentage") or {}).get("eth") or 0.0)
        eth_cap = total * ethd / 100.0 if total > 0 else 0.0

        total2 = total - btc_cap
        total3 = total - btc_cap - eth_cap
        others = total2
        othersd = (others / total * 100.0) if total > 0 else 0.0

        ts = int(m.get("timestamp") or 0)
        if ts <= 0:
            continue
        extra = json.dumps(
            {
                "market_cap_total": total,
                "market_cap_btc": btc_cap,
                "market_cap_eth": eth_cap,
                "btcd": btcd,
                "ethd": ethd,
            },
            ensure_ascii=True,
        )

        values = {
            "TOTAL": total,
            "TOTAL2": total2,
            "TOTAL3": total3,
            "BTCD": btcd,
            "OTHERS": others,
            "OTHERSD": othersd,
        }
        for symbol, value in values.items():
            daily_by_symbol[symbol].append(
                {
                    "timestamp": ts,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "volume": 0.0 if symbol in {"BTCD", "OTHERSD"} else total,
                    "source": SOURCE_COINGECKO_AGG,
                    "extra": extra,
                }
            )

    for symbol, daily_rows in daily_by_symbol.items():
        if not daily_rows:
            continue
        daily_rows = sorted(daily_rows, key=lambda r: int(r["timestamp"]))
        db_client.save_ohlcv(symbol, "1d", daily_rows)
        max_daily_ts = max(int(r["timestamp"]) for r in daily_rows)
        last_daily = db_client.get_last_update(symbol, "1d", source=SOURCE_COINGECKO_AGG)
        if last_daily is None:
            db_client.update_metadata(symbol, "1d", max_daily_ts, max_daily_ts, source=SOURCE_COINGECKO_AGG)
        else:
            db_client.update_metadata(symbol, "1d", max_daily_ts, source=SOURCE_COINGECKO_AGG)

        df = pd.DataFrame(daily_rows)
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["week_start"] = df["dt"].dt.normalize() - pd.to_timedelta(df["dt"].dt.weekday, unit="D")
        weekly_rows: List[Dict[str, Any]] = []
        for wk, grp in df.groupby("week_start", sort=True):
            grp = grp.sort_values("dt")
            weekly_rows.append(
                {
                    "timestamp": int(pd.Timestamp(wk).timestamp()),
                    "open": float(grp.iloc[0]["open"]),
                    "high": float(grp["high"].max()),
                    "low": float(grp["low"].min()),
                    "close": float(grp.iloc[-1]["close"]),
                    "volume": float(grp["volume"].sum()),
                    "source": SOURCE_COINGECKO_AGG,
                    "extra": grp.iloc[-1]["extra"],
                }
            )
        if weekly_rows:
            db_client.save_ohlcv(symbol, "1w", weekly_rows)
            max_weekly_ts = max(int(r["timestamp"]) for r in weekly_rows)
            last_weekly = db_client.get_last_update(symbol, "1w", source=SOURCE_COINGECKO_AGG)
            if last_weekly is None:
                db_client.update_metadata(symbol, "1w", max_weekly_ts, max_weekly_ts, source=SOURCE_COINGECKO_AGG)
            else:
                db_client.update_metadata(symbol, "1w", max_weekly_ts, source=SOURCE_COINGECKO_AGG)

def update_futures_open_interest(symbol: str, period: str = "4h", days_back: int = 30) -> None:
    """
    Update open interest from Binance futures and save to open_interest table.
    """
    client = get_exchange_client(SOURCE_BINANCE)
    futures_symbol = client.to_futures_symbol(symbol)
    now_ts = int(time.time())
    tf = _normalize_timeframe(period)
    meta_symbol = _meta_symbol_key("open_interest", symbol)
    last_ts = db_client.get_last_update(meta_symbol, tf, source=DEFAULT_SOURCE_BINANCE)
    since = _HISTORY_START_TS if last_ts is None else int(last_ts) + 1
    since = max(since, now_ts - days_back * 86400)
    if since >= now_ts:
        return

    rows = client.fetch_open_interest_history(symbol=futures_symbol, period=tf, limit=500)
    if not rows:
        return
    filtered = [r for r in rows if int(r["timestamp"]) >= since]
    if not filtered:
        return

    db_client.save_open_interest(
        symbol=symbol,
        timeframe=tf,
        records=[
            {
                "timestamp": r["timestamp"],
                "exchange": "binance",
                "oi_value": r["oi_value"],
                "oi_change_24h": r["oi_change_24h"],
            }
            for r in filtered
        ],
        source=DEFAULT_SOURCE_BINANCE,
    )
    max_ts = max(int(r["timestamp"]) for r in filtered)
    if last_ts is None:
        db_client.update_metadata(meta_symbol, tf, max_ts, max_ts, source=DEFAULT_SOURCE_BINANCE)
    else:
        db_client.update_metadata(meta_symbol, tf, max_ts, source=DEFAULT_SOURCE_BINANCE)


def update_liquidation_history(symbol: str, timeframe: str = "4h", days_back: int = 30) -> None:
    """
    Update liquidation history from Binance futures and aggregate into timeframe buckets.
    """
    client = get_exchange_client(SOURCE_BINANCE)
    futures_symbol = client.to_futures_symbol(symbol)
    now_ts = int(time.time())
    tf = _normalize_timeframe(timeframe)
    tf_seconds = {"1h": 3600, "4h": 14400, "1d": 86400}.get(tf, 14400)

    meta_symbol = _meta_symbol_key("liquidations", symbol)
    last_ts = db_client.get_last_update(meta_symbol, tf, source=DEFAULT_SOURCE_BINANCE)
    since = _HISTORY_START_TS if last_ts is None else int(last_ts) + 1
    since = max(since, now_ts - days_back * 86400)
    if since >= now_ts:
        return

    raw_orders = client.fetch_liquidation_orders(symbol=futures_symbol, start_time=since, end_time=now_ts, limit=1000)
    if not raw_orders:
        return

    buckets: Dict[int, Dict[str, Any]] = {}
    for row in raw_orders:
        ts = int(row["timestamp"])
        bucket_ts = (ts // tf_seconds) * tf_seconds
        qty = float(row.get("original_quantity") or 0.0)
        price = float(row.get("price") or 0.0)
        notional = qty * price if price > 0 else qty
        side = str(row.get("side") or "").upper()
        b = buckets.setdefault(
            bucket_ts,
            {"timestamp": bucket_ts, "exchange": "binance", "long_volume": 0.0, "short_volume": 0.0, "total_volume": 0.0},
        )
        if side == "BUY":
            b["short_volume"] += notional
        elif side == "SELL":
            b["long_volume"] += notional
        else:
            b["total_volume"] += notional
        b["total_volume"] = b["long_volume"] + b["short_volume"] if (b["long_volume"] + b["short_volume"]) > 0 else b["total_volume"]

    records = [buckets[k] for k in sorted(buckets.keys()) if buckets[k]["timestamp"] >= since]
    if not records:
        return
    db_client.save_liquidations(symbol=symbol, timeframe=tf, records=records, source=DEFAULT_SOURCE_BINANCE)
    max_ts = max(int(r["timestamp"]) for r in records)
    if last_ts is None:
        db_client.update_metadata(meta_symbol, tf, max_ts, max_ts, source=DEFAULT_SOURCE_BINANCE)
    else:
        db_client.update_metadata(meta_symbol, tf, max_ts, source=DEFAULT_SOURCE_BINANCE)


def update_all_futures_data(days_back: int = 30) -> None:
    """
    Binance USDT-M: только open interest по TRADING_SYMBOLS.

    Ликвидации в проекте собираются с Bybit (WebSocket), см. `DataLoaderManager.update_liquidations`
    и `load_all_data.py` — не дублируем Binance REST (нужны API keys и другой смысл `source`).
    """
    for symbol in TRADING_SYMBOLS:
        try:
            for tf in ["1h", "4h", "1d"]:
                update_futures_open_interest(symbol=symbol, period=tf, days_back=days_back)
                time.sleep(_SYMBOL_SLEEP_SEC)
        except Exception as exc:
            logger.exception("Futures update failed for %s: %s", symbol, exc)
