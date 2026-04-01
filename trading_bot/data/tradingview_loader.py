"""
Загрузка OHLCV индексов CRYPTOCAP через TradingView (tvDatafeed).
Времена нормализуются по сетке закрытия (1m / 4h / день / неделя с понедельника / месяц).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.config.settings import (
    DEFAULT_SOURCE_TRADINGVIEW,
    TRADINGVIEW_EXCHANGE,
    TRADINGVIEW_MAX_BARS,
    TRADINGVIEW_PASSWORD,
    TRADINGVIEW_SYMBOLS,
    TRADINGVIEW_USERNAME,
    TRADINGVIEW_WS_TIMEOUT,
)
from trading_bot.data.base_loader import BaseDataLoader

logger = logging.getLogger(__name__)


def _normalize_to_day_start(dt: pd.Timestamp) -> pd.Timestamp:
    t = dt.to_pydatetime()
    return pd.Timestamp(
        t.replace(hour=0, minute=0, second=0, microsecond=0),
    )


def _normalize_to_4h_start(dt: pd.Timestamp) -> pd.Timestamp:
    t = dt.to_pydatetime()
    return pd.Timestamp(
        t.replace(hour=(t.hour // 4) * 4, minute=0, second=0, microsecond=0),
    )


def _normalize_to_minute_start(dt: pd.Timestamp) -> pd.Timestamp:
    t = dt.to_pydatetime()
    return pd.Timestamp(
        t.replace(second=0, microsecond=0),
    )


def _normalize_to_week_start(dt: pd.Timestamp) -> pd.Timestamp:
    t = dt.to_pydatetime()
    base = t - timedelta(days=t.weekday())
    return pd.Timestamp(
        base.replace(hour=0, minute=0, second=0, microsecond=0),
    )


def _normalize_to_month_start(dt: pd.Timestamp) -> pd.Timestamp:
    t = dt.to_pydatetime()
    return pd.Timestamp(
        t.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
    )


def _patch_tvdatafeed_websocket_timeout(seconds: int) -> None:
    """tvDatafeed использует __ws_timeout = 5 с на классе — увеличиваем до seconds."""
    try:
        import tvDatafeed.main as tvm  # type: ignore

        sec = max(5, int(seconds))
        setattr(tvm.TvDatafeed, "_TvDatafeed__ws_timeout", sec)
        logger.debug("tvDatafeed WebSocket timeout set to %ss", sec)
    except Exception as exc:
        logger.warning("Could not patch tvDatafeed ws timeout: %s", exc)


_TIMEFRAME_TV = {
    "1m": {"normalize": _normalize_to_minute_start},
    "4h": {"normalize": _normalize_to_4h_start},
    "1d": {"normalize": _normalize_to_day_start},
    "1w": {"normalize": _normalize_to_week_start},
    "1M": {"normalize": _normalize_to_month_start},
}


class TradingViewDataLoader(BaseDataLoader):
    def __init__(
        self,
        symbol_map: Optional[Dict[str, str]] = None,
        exchange: str = TRADINGVIEW_EXCHANGE,
        n_bars_cap: int = TRADINGVIEW_MAX_BARS,
    ) -> None:
        try:
            from tvDatafeed import Interval, TvDatafeed  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "tvDatafeed is required. Install with `pip install tvdatafeed`."
            ) from exc

        _patch_tvdatafeed_websocket_timeout(TRADINGVIEW_WS_TIMEOUT)

        self._Interval = Interval
        self._TvDatafeed = TvDatafeed
        if TRADINGVIEW_USERNAME and TRADINGVIEW_PASSWORD:
            self._tv = TvDatafeed(username=TRADINGVIEW_USERNAME, password=TRADINGVIEW_PASSWORD)
            if getattr(self._tv, "token", None) == "unauthorized_user_token":
                logger.warning(
                    "TradingView: вход через tvdatafeed не удалился (часто из‑за 2FA, email vs логин, "
                    "или смены API signin на стороне TradingView). Идём в nologin + увеличенный WS timeout."
                )
            else:
                logger.info("TradingView: получен auth_token (tvDatafeed)")
        else:
            self._tv = TvDatafeed()
            logger.warning(
                "TradingView: TRADINGVIEW_USERNAME/PASSWORD not set — nologin (limited data). "
                "Put them in .env at project root or export in the shell."
            )
        self._symbol_map = dict(symbol_map or TRADINGVIEW_SYMBOLS)
        self._exchange = exchange
        self._n_bars_cap = n_bars_cap

    def get_exchange_name(self) -> str:
        return DEFAULT_SOURCE_TRADINGVIEW

    def _interval_for_timeframe(self, timeframe: str):
        tf = timeframe if timeframe != "1W" else "1w"
        if tf == "1m":
            return self._Interval.in_1_minute
        if tf == "4h":
            return self._Interval.in_4_hour
        if tf == "1d":
            return self._Interval.in_daily
        if tf == "1w":
            return self._Interval.in_weekly
        if tf == "1M":
            return self._Interval.in_monthly
        raise ValueError(f"TradingView loader: unsupported timeframe {timeframe!r}")

    def _tv_symbol(self, symbol: str) -> str:
        if symbol not in self._symbol_map:
            raise ValueError(f"No TradingView mapping for symbol {symbol!r}")
        return self._symbol_map[symbol]

    def _estimate_bars(self, timeframe: str, start_ts: Optional[int], end_ts: Optional[int]) -> int:
        if start_ts is None or end_ts is None:
            return self._n_bars_cap
        secs = max(0, int(end_ts) - int(start_ts))
        tf = timeframe if timeframe != "1W" else "1w"
        if tf == "1m":
            est = secs // 60 + 5
        elif tf == "4h":
            est = secs // (4 * 3600) + 5
        elif tf == "1d":
            est = secs // 86400 + 5
        elif tf == "1w":
            est = secs // (86400 * 7) + 5
        else:
            est = secs // (86400 * 28) + 5
        return max(50, min(self._n_bars_cap, est))

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        tf = timeframe if timeframe != "1W" else "1w"
        if tf not in _TIMEFRAME_TV:
            raise ValueError(f"Unsupported timeframe for TradingView: {timeframe}")

        tv_sym = self._tv_symbol(symbol)
        interval = self._interval_for_timeframe(tf)
        n_bars = self._estimate_bars(tf, start_ts, end_ts)

        @retry(
            reraise=True,
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=1, min=2, max=45),
            retry=retry_if_exception_type((TimeoutError, OSError, ConnectionError)),
        )
        def _get_hist() -> pd.DataFrame:
            return self._tv.get_hist(
                symbol=tv_sym, exchange=self._exchange, interval=interval, n_bars=n_bars
            )

        df = _get_hist()
        if df is None or df.empty:
            return []

        df = df.copy()
        df.index = pd.to_datetime(df.index)
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        now_utc = datetime.now(timezone.utc)
        df = df[df.index <= pd.Timestamp(now_utc)]

        norm_fn = _TIMEFRAME_TV[tf]["normalize"]
        df.index = df.index.map(norm_fn)
        df = df[~df.index.duplicated(keep="first")]

        # Не использовать .view("int64") на tz-aware DatetimeIndex — в pandas 2.x даёт неверные секунды.
        idx_unix = np.array([int(pd.Timestamp(x).timestamp()) for x in df.index], dtype=np.int64)
        if start_ts is not None:
            df = df[idx_unix >= int(start_ts)]
            idx_unix = np.array([int(pd.Timestamp(x).timestamp()) for x in df.index], dtype=np.int64)
        if end_ts is not None:
            df = df[idx_unix <= int(end_ts)]

        df = df.sort_index()

        src = self.get_exchange_name()
        records: List[Dict[str, Any]] = []
        for ts, row in df.iterrows():
            t_unix = int(ts.timestamp())

            def _f(col: str) -> Optional[float]:
                v = row[col] if col in row.index else None
                if v is None or pd.isna(v):
                    return None
                return float(v)

            vol_raw = row["volume"] if "volume" in row.index else 0.0
            vol_f = 0.0 if pd.isna(vol_raw) else float(vol_raw)

            records.append(
                {
                    "timestamp": t_unix,
                    "open": _f("open"),
                    "high": _f("high"),
                    "low": _f("low"),
                    "close": _f("close"),
                    "volume": vol_f,
                    "source": src,
                }
            )

        return records
