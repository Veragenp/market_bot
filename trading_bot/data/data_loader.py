"""Фасад загрузки данных (репозитории + загрузчики)."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Optional

from trading_bot.analytics.atr import GERCHIK_ATR_BARS, atr_gerchik_from_ohlcv_rows
from trading_bot.config.settings import (
    DEFAULT_SOURCE_BINANCE,
    HISTORY_START_TS,
    INTRADAY_1M_DAYS,
    INSTRUMENTS_SYMBOLS_TO_UPDATE,
    MIN_AVG_VOLUME_24H,
    OI_HISTORY_DAYS,
    OI_TIMEFRAMES,
    LIQUIDATIONS_AGGREGATE_TIMEFRAMES,
    SOURCE_TRADINGVIEW,
    TIMEFRAMES_BY_CATEGORY,
)
from trading_bot.config.symbols import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.data.binance_spot_loader import BinanceSpotDataLoader
from trading_bot.data.bybit_futures_loader import BybitFuturesDataLoader
from trading_bot.data.repositories import (
    MetadataRepository,
    OHLCVRepository,
    InstrumentsRepository,
    OIRepository,
    LiquidationsRepository,
    get_ohlcv,
    get_ohlcv_tail,
)
from trading_bot.data.tradingview_loader import TradingViewDataLoader
from trading_bot.data.yahoo_finance_loader import YahooFinanceDataLoader

logger = logging.getLogger(__name__)


def _end_closed_1m(now_ts: int) -> int:
    # last fully closed 1m candle start time
    return now_ts - (now_ts % 60) - 60


class DataLoaderManager:
    def __init__(self) -> None:
        self.ohlcv_repo = OHLCVRepository()
        self.meta_repo = MetadataRepository()
        self.instruments_repo = InstrumentsRepository()
        self.oi_repo = OIRepository()
        self.liquidations_repo = LiquidationsRepository()

        self.spot_loader = BinanceSpotDataLoader()
        self.macro_loader = YahooFinanceDataLoader()
        self.tv_loader = TradingViewDataLoader()
        self.bybit_loader = BybitFuturesDataLoader()

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        source: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return list(get_ohlcv(symbol, timeframe, start=start, end=end, source=source))

    def get_current_price(self, symbol: str) -> float:
        return self.bybit_loader.get_current_price(symbol)

    def load_instruments_futures(self, symbols: Optional[List[str]] = None) -> None:
        """Заполнить таблицу `instruments` для фьючерсов Bybit."""
        symbols = symbols or TRADING_SYMBOLS
        for symbol in symbols:
            data = self.bybit_loader.fetch_instrument_info(symbol)
            # `fetch_instrument_info` возвращает symbol в формате Bybit.
            bybit_symbol = data.get("symbol") or symbol.replace("/", "").upper()
            self.instruments_repo.save_or_update(
                symbol=str(bybit_symbol),
                exchange="bybit_futures",
                data=data,
            )
            logger.info("Instruments loaded: %s", symbol)

    @staticmethod
    def _to_bybit_symbol(symbol_internal: str) -> str:
        return symbol_internal.replace("/", "").upper()

    @staticmethod
    def _from_bybit_symbol(symbol_bybit: str) -> str:
        # common for linear USDT perpetual
        if symbol_bybit.endswith("USDT"):
            base = symbol_bybit[: -len("USDT")]
            return f"{base}/USDT"
        return symbol_bybit

    def update_instruments_full(self) -> int:
        """
        Обновляет таблицу `instruments` для всех/нужных фьючерсных инструментов Bybit.
        Возвращает количество обновлённых записей.
        """
        symbols_to_update = INSTRUMENTS_SYMBOLS_TO_UPDATE
        records = self.bybit_loader.fetch_all_instruments_info(symbols_to_filter=symbols_to_update)
        for rec in records:
            self.instruments_repo.save_or_update(
                symbol=str(rec["symbol"]),
                exchange="bybit_futures",
                data=rec,
            )
        return len(records)

    def update_instruments_for_symbols(self, symbols_list: List[str]) -> int:
        """Обновляет таблицу `instruments` только для заданных TRADING_SYMBOLS (internal format)."""
        records = self.bybit_loader.fetch_all_instruments_info(symbols_to_filter=symbols_list)
        for rec in records:
            self.instruments_repo.save_or_update(
                symbol=str(rec["symbol"]),
                exchange="bybit_futures",
                data=rec,
            )
        return len(records)

    def update_instruments_atr_for_trading_symbols(
        self,
        *,
        source: str = DEFAULT_SOURCE_BINANCE,
        ohlcv_limit: int = 400,
    ) -> int:
        """
        Единственная запись дневного ATR в торговый контур: колонка `instruments.atr` (bybit_futures).
        Остальной код (cycle_levels, VP, level_events) только читает это поле.

        Метод: Герчик по **последним 10** дневным spot-свечам из SQLite (`get_ohlcv_tail`), без REST
        на этом шаге. Строка в `instruments` должна уже существовать.
        """
        updated = 0
        for symbol_internal in TRADING_SYMBOLS:
            bybit_sym = self._to_bybit_symbol(symbol_internal)
            if not self.instruments_repo.get(bybit_sym, "bybit_futures"):
                logger.warning("ATR skip %s: no instruments row for %s", symbol_internal, bybit_sym)
                continue
            rows = get_ohlcv_tail(
                symbol_internal,
                "1d",
                limit=ohlcv_limit,
                source=source,
            )
            atr_val = atr_gerchik_from_ohlcv_rows(rows)
            if atr_val is None:
                logger.warning(
                    "ATR skip %s: insufficient 1d OHLCV (source=%s, rows=%s, need>=%s)",
                    symbol_internal,
                    source,
                    len(rows),
                    GERCHIK_ATR_BARS,
                )
                continue
            self.instruments_repo.update_atr(bybit_sym, "bybit_futures", atr_val)
            updated += 1
            logger.info("ATR updated %s -> %s (gerchik, last %s bars)", symbol_internal, atr_val, GERCHIK_ATR_BARS)
        return updated

    def update_instruments_daily(self) -> None:
        """Bybit поля инструмента + ATR по TRADING_SYMBOLS (для ежедневного job)."""
        n_inst = self.update_instruments_for_symbols(list(TRADING_SYMBOLS))
        n_atr = self.update_instruments_atr_for_trading_symbols()
        logger.info("Daily instruments: bybit rows=%s, atr_updated=%s", n_inst, n_atr)

    def is_tradable(self, symbol_internal: str) -> bool:
        """Фильтр по ликвидности (avg_volume_24h в USDT)."""
        bybit_symbol = self._to_bybit_symbol(symbol_internal)
        rec = self.instruments_repo.get(bybit_symbol, "bybit_futures")
        if not rec:
            return False
        avg = rec.get("avg_volume_24h")
        if avg is None:
            return False
        try:
            return float(avg) >= float(MIN_AVG_VOLUME_24H)
        except (TypeError, ValueError):
            return False

    def get_liquid_symbols_from_instruments(self) -> List[str]:
        """Список internal символов (например BTC/USDT) с avg_volume_24h >= порога."""
        bybit_symbols = self.instruments_repo.get_symbols_with_min_avg_volume(
            min_avg_volume_24h=MIN_AVG_VOLUME_24H,
            exchange="bybit_futures",
        )
        return [self._from_bybit_symbol(s) for s in bybit_symbols]

    def _save_records(
        self,
        *,
        symbol: str,
        timeframe: str,
        source: str,
        records: List[Dict[str, Any]],
        force_full: bool,
    ) -> None:
        if not records:
            return
        self.ohlcv_repo.save_batch(symbol, timeframe, records)
        max_ts = max(int(r["timestamp"]) for r in records)
        now_ts = int(time.time())
        if force_full:
            self.meta_repo.update(symbol, timeframe, source=source, last_updated=max_ts, last_full_update=now_ts)
        else:
            self.meta_repo.update(symbol, timeframe, source=source, last_updated=max_ts)

    def load_historical_spot(
        self,
        symbols: Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
        *,
        force_full: bool = True,
        end_ts: Optional[int] = None,
    ) -> None:
        symbols = symbols or TRADING_SYMBOLS
        if timeframes is None:
            timeframes = list(TIMEFRAMES_BY_CATEGORY["spot"]["historical"])  # type: ignore[index]
        end_ts = end_ts if end_ts is not None else int(time.time())

        source = self.spot_loader.get_exchange_name()
        for symbol in symbols:
            for timeframe in timeframes:
                start_ts = HISTORY_START_TS if force_full else (self.meta_repo.get_last_updated(symbol, timeframe, source) or (HISTORY_START_TS - 1)) + 1
                records = self.spot_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_ts)
                self._save_records(
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=force_full,
                )
                logger.info("Spot historical loaded: %s %s rows=%s", symbol, timeframe, len(records))

    def load_intraday_1m_spot(
        self,
        symbols: Optional[List[str]] = None,
        *,
        days_back: int = INTRADAY_1M_DAYS,
        force_full: bool = True,
        end_ts: Optional[int] = None,
    ) -> None:
        symbols = symbols or TRADING_SYMBOLS
        now_ts = int(time.time()) if end_ts is None else int(end_ts)
        end_closed = _end_closed_1m(now_ts)

        start_ts = end_closed - int(days_back) * 86400
        timeframe = "1m"
        source = self.spot_loader.get_exchange_name()

        for symbol in symbols:
            records = self.spot_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_closed)
            self._save_records(
                symbol=symbol,
                timeframe=timeframe,
                source=source,
                records=records,
                force_full=force_full,
            )
            logger.info("Spot 1m loaded: %s rows=%s", symbol, len(records))

    def load_historical_macro(
        self,
        symbols: Optional[List[str]] = None,
        *,
        force_full: bool = True,
        end_ts: Optional[int] = None,
    ) -> None:
        symbols = symbols or ANALYTIC_SYMBOLS.get("macro", [])
        timeframes = list(TIMEFRAMES_BY_CATEGORY["macro"])  # type: ignore[index]
        end_ts = end_ts if end_ts is not None else int(time.time())

        source = self.macro_loader.get_exchange_name()
        for symbol in symbols:
            for timeframe in timeframes:
                start_ts = HISTORY_START_TS if force_full else (self.meta_repo.get_last_updated(symbol, timeframe, source) or (HISTORY_START_TS - 1)) + 1
                records = self.macro_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_ts)
                self._save_records(
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=force_full,
                )
                logger.info("Macro historical loaded: %s %s rows=%s", symbol, timeframe, len(records))

    def load_historical_tradingview_indices(
        self,
        symbols: Optional[List[str]] = None,
        *,
        force_full: bool = True,
        end_ts: Optional[int] = None,
    ) -> None:
        symbols = symbols or ANALYTIC_SYMBOLS.get("indices", [])
        timeframes = list(TIMEFRAMES_BY_CATEGORY["indices"])  # type: ignore[index]
        end_ts = end_ts if end_ts is not None else int(time.time())

        source = self.tv_loader.get_exchange_name()
        for symbol in symbols:
            for timeframe in timeframes:
                start_ts = HISTORY_START_TS if force_full else (self.meta_repo.get_last_updated(symbol, timeframe, source) or (HISTORY_START_TS - 1)) + 1
                records = self.tv_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_ts)
                self._save_records(
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=force_full,
                )
                logger.info("TradingView indices loaded: %s %s rows=%s", symbol, timeframe, len(records))

    def update_incremental_spot(
        self,
        symbols: Optional[List[str]] = None,
        *,
        timeframes: Optional[List[str]] = None,
        days_back_for_1m: int = INTRADAY_1M_DAYS,
    ) -> None:
        symbols = symbols or TRADING_SYMBOLS
        source = self.spot_loader.get_exchange_name()
        if timeframes is None:
            timeframes = list(TIMEFRAMES_BY_CATEGORY["spot"]["historical"]) + ["1m"]  # type: ignore[index]

        now_ts = int(time.time())
        for symbol in symbols:
            for timeframe in timeframes:
                last_ts = self.meta_repo.get_last_updated(symbol, timeframe, source)
                if timeframe == "1m":
                    default_start = now_ts - int(days_back_for_1m) * 86400
                    end_closed = _end_closed_1m(now_ts)
                    start_ts = (last_ts + 1) if last_ts is not None else default_start
                    records = self.spot_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_closed)
                else:
                    end_ts = now_ts
                    start_ts = (last_ts + 1) if last_ts is not None else HISTORY_START_TS
                    records = self.spot_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=end_ts)

                self._save_records(
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=False,
                )
                if records:
                    logger.info("Spot incremental loaded: %s %s rows=%s", symbol, timeframe, len(records))

    def update_incremental_macro(
        self,
        symbols: Optional[List[str]] = None,
    ) -> None:
        symbols = symbols or ANALYTIC_SYMBOLS.get("macro", [])
        timeframes = list(TIMEFRAMES_BY_CATEGORY["macro"])  # type: ignore[index]
        source = self.macro_loader.get_exchange_name()
        now_ts = int(time.time())

        for symbol in symbols:
            for timeframe in timeframes:
                last_ts = self.meta_repo.get_last_updated(symbol, timeframe, source)
                start_ts = (last_ts + 1) if last_ts is not None else HISTORY_START_TS
                records = self.macro_loader.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start_ts=start_ts, end_ts=now_ts)
                self._save_records(
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=False,
                )
                if records:
                    logger.info("Macro incremental loaded: %s %s rows=%s", symbol, timeframe, len(records))

    def _save_oi_records(
        self,
        *,
        meta_symbol: str,
        symbol: str,
        timeframe: str,
        source: str,
        records: List[Dict[str, Any]],
        force_full: bool,
    ) -> None:
        if not records:
            return
        self.oi_repo.save_batch(symbol, timeframe, records=records, source=source)
        max_ts = max(int(r["timestamp"]) for r in records)
        now_ts = int(time.time())
        if force_full:
            self.meta_repo.update(meta_symbol, timeframe, source=source, last_updated=max_ts, last_full_update=now_ts)
        else:
            self.meta_repo.update(meta_symbol, timeframe, source=source, last_updated=max_ts)

    def load_historical_oi(
        self,
        symbols: Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
        *,
        end_ts: Optional[int] = None,
    ) -> None:
        """Load Bybit OI history into `open_interest` and update `metadata`."""
        symbols = symbols or TRADING_SYMBOLS
        timeframes = timeframes or list(OI_TIMEFRAMES)

        now_ts = int(time.time()) if end_ts is None else int(end_ts)
        start_ts = now_ts - int(OI_HISTORY_DAYS) * 86400
        source = self.bybit_loader.get_exchange_name()

        for symbol in symbols:
            meta_symbol = f"open_interest:{symbol}"
            for timeframe in timeframes:
                records = self.bybit_loader.fetch_open_interest(
                    symbol=symbol,
                    interval=timeframe,
                    start_ts=start_ts,
                    end_ts=now_ts,
                )
                self._save_oi_records(
                    meta_symbol=meta_symbol,
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=True,
                )
                logger.info(
                    "Open interest historical loaded: %s %s rows=%s",
                    symbol,
                    timeframe,
                    len(records),
                )

    def update_incremental_oi(
        self,
        symbols: Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
    ) -> None:
        """Incremental OI update using `metadata.open_interest:*` as the cursor."""
        symbols = symbols or TRADING_SYMBOLS
        timeframes = timeframes or list(OI_TIMEFRAMES)
        source = self.bybit_loader.get_exchange_name()
        now_ts = int(time.time())

        for symbol in symbols:
            meta_symbol = f"open_interest:{symbol}"
            for timeframe in timeframes:
                last_ts = self.meta_repo.get_last_updated(meta_symbol, timeframe, source)
                default_start = now_ts - int(OI_HISTORY_DAYS) * 86400
                start_ts = (int(last_ts) + 1) if last_ts is not None else default_start

                records = self.bybit_loader.fetch_open_interest(
                    symbol=symbol,
                    interval=timeframe,
                    start_ts=start_ts,
                    end_ts=now_ts,
                )
                if not records:
                    continue
                self._save_oi_records(
                    meta_symbol=meta_symbol,
                    symbol=symbol,
                    timeframe=timeframe,
                    source=source,
                    records=records,
                    force_full=last_ts is None,
                )
                logger.info(
                    "Open interest incremental loaded: %s %s rows=%s",
                    symbol,
                    timeframe,
                    len(records),
                )

    @staticmethod
    def _aggregate_liquidations_events(
        events: List[Dict[str, Any]],
        timeframe: str,
        *, exchange: str,
    ) -> List[Dict[str, Any]]:
        tf_seconds = {"1h": 3600, "4h": 14400, "1d": 86400}.get(timeframe)
        if tf_seconds is None:
            raise ValueError(f"Unsupported liquidations aggregation timeframe: {timeframe}")

        buckets: Dict[int, Dict[str, Any]] = {}
        for ev in events:
            ts = int(ev["timestamp"])
            bucket_ts = (ts // tf_seconds) * tf_seconds
            side = str(ev.get("side") or "").upper()
            qty = float(ev.get("qty") or 0.0)
            price = float(ev.get("price") or 0.0)
            notional = qty * price if price > 0 else qty

            b = buckets.setdefault(
                bucket_ts,
                {
                    "timestamp": bucket_ts,
                    "exchange": exchange,
                    "long_volume": 0.0,
                    "short_volume": 0.0,
                    "total_volume": 0.0,
                },
            )

            if side == "BUY":
                b["short_volume"] += notional
            elif side == "SELL":
                b["long_volume"] += notional
            else:
                b["total_volume"] += notional

            # Keep total consistent for expected Buy/Sell sides.
            if (b["long_volume"] + b["short_volume"]) > 0:
                b["total_volume"] = b["long_volume"] + b["short_volume"]

        out = [buckets[k] for k in sorted(buckets.keys())]
        return out

    def update_liquidations(
        self,
        symbols: Optional[List[str]] = None,
        aggregate_timeframes: Optional[List[str]] = None,
    ) -> None:
        """
        Update stored liquidations in `liquidations` using latest collected events.

        NOTE: Bybit liquidation feed provides only fresh events via WebSocket; we use `metadata`
        as an approximate cursor and filter buckets strictly greater than `last_updated`.
        """
        symbols = symbols or TRADING_SYMBOLS
        aggregate_timeframes = aggregate_timeframes or list(LIQUIDATIONS_AGGREGATE_TIMEFRAMES)
        source = self.bybit_loader.get_exchange_name()
        now_ts = int(time.time())

        for symbol in symbols:
            meta_symbol = f"liquidations:{symbol}"
            raw_events = self.bybit_loader.fetch_liquidations(symbol=symbol)
            if not raw_events:
                continue

            for timeframe in aggregate_timeframes:
                last_ts = self.meta_repo.get_last_updated(meta_symbol, timeframe, source)
                records = self._aggregate_liquidations_events(raw_events, timeframe, exchange=source)

                if last_ts is not None:
                    last_ts_i = int(last_ts)
                    records = [r for r in records if int(r["timestamp"]) > last_ts_i]

                if not records:
                    continue

                self.liquidations_repo.save_batch(symbol, timeframe, records=records, source=source)
                max_ts = max(int(r["timestamp"]) for r in records)
                if last_ts is None:
                    self.meta_repo.update(meta_symbol, timeframe, source=source, last_updated=max_ts, last_full_update=now_ts)
                else:
                    self.meta_repo.update(meta_symbol, timeframe, source=source, last_updated=max_ts)

                logger.info(
                    "Liquidations updated: %s %s buckets=%s",
                    symbol,
                    timeframe,
                    len(records),
                )
