"""
DAO-слой: доступ к БД. Используется db_client и DataLoaderManager.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from trading_bot.config.settings import DEFAULT_SOURCE_BINANCE, YFINANCE_TICKERS
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db


def _ensure_db() -> None:
    init_db()


def save_ohlcv(symbol: str, timeframe: str, records: List[Dict[str, Any]]) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    for rec in records:
        cursor.execute(
            """
            INSERT OR REPLACE INTO ohlcv
            (symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                timeframe,
                rec["timestamp"],
                rec.get("open"),
                rec.get("high"),
                rec.get("low"),
                rec.get("close"),
                rec.get("volume"),
                rec.get("source"),
                rec.get("extra"),
                now,
            ),
        )
    conn.commit()
    conn.close()


def get_ohlcv(
    symbol: str,
    timeframe: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    limit: Optional[int] = None,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = ?
    """
    params: List[Any] = [symbol, timeframe]
    if source is not None:
        query += " AND ifnull(source, '') = ?"
        params.append(source)
    if start is not None:
        query += " AND timestamp >= ?"
        params.append(start)
    if end is not None:
        query += " AND timestamp <= ?"
        params.append(end)
    query += " ORDER BY timestamp"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_ohlcv_filled(
    symbol: str,
    timeframe: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    limit: Optional[int] = None,
    fill_weekends: bool = True,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    records = get_ohlcv(symbol=symbol, timeframe=timeframe, start=start, end=end, limit=limit, source=source)
    if not records:
        return records

    is_macro = symbol in YFINANCE_TICKERS
    is_daily = timeframe == "1d"
    if not (fill_weekends and is_macro and is_daily):
        return records

    day_step = 86400
    by_ts: Dict[int, Dict[str, Any]] = {int(row["timestamp"]): row for row in records}
    sorted_ts = sorted(by_ts.keys())

    range_start = start if start is not None else sorted_ts[0]
    range_end = end if end is not None else sorted_ts[-1]
    range_start = int(range_start // day_step * day_step)
    range_end = int(range_end // day_step * day_step)

    prev_close = None
    if start is not None:
        _ensure_db()
        conn = get_connection()
        cursor = conn.cursor()
        q = """
            SELECT close
            FROM ohlcv
            WHERE symbol = ? AND timeframe = ? AND timestamp < ?
        """
        p: List[Any] = [symbol, timeframe, range_start]
        if source is not None:
            q += " AND ifnull(source, '') = ?"
            p.append(source)
        q += " ORDER BY timestamp DESC LIMIT 1"
        cursor.execute(q, p)
        prev_row = cursor.fetchone()
        conn.close()
        if prev_row:
            prev_close = prev_row[0]

    filled: List[Dict[str, Any]] = []
    current = range_start
    while current <= range_end:
        existing = by_ts.get(current)
        if existing is not None:
            filled.append(existing)
            if existing.get("close") is not None:
                prev_close = existing["close"]
        else:
            if prev_close is not None:
                filled.append(
                    {
                        "timestamp": current,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": prev_close,
                        "volume": None,
                    }
                )
        current += day_step

    if limit is not None:
        return filled[:limit]
    return filled


def get_last_update(symbol: str, timeframe: str, source: str = DEFAULT_SOURCE_BINANCE) -> Optional[int]:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT last_updated FROM metadata
        WHERE symbol = ? AND timeframe = ? AND source = ?
        """,
        (symbol, timeframe, source),
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def update_metadata(
    symbol: str,
    timeframe: str,
    last_updated: int,
    last_full_update: Optional[int] = None,
    source: str = DEFAULT_SOURCE_BINANCE,
) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT last_updated, last_full_update FROM metadata
        WHERE symbol = ? AND timeframe = ? AND source = ?
        """,
        (symbol, timeframe, source),
    )
    row = cursor.fetchone()
    if row:
        new_full = last_full_update if last_full_update is not None else row[1]
        cursor.execute(
            """
            UPDATE metadata
            SET last_updated = ?, last_full_update = ?
            WHERE symbol = ? AND timeframe = ? AND source = ?
            """,
            (last_updated, new_full, symbol, timeframe, source),
        )
    else:
        cursor.execute(
            """
            INSERT INTO metadata (symbol, timeframe, source, last_updated, last_full_update)
            VALUES (?, ?, ?, ?, ?)
            """,
            (symbol, timeframe, source, last_updated, last_full_update),
        )
    conn.commit()
    conn.close()


def set_last_cleaned(symbol: str, timeframe: str, timestamp: int, source: str = DEFAULT_SOURCE_BINANCE) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE metadata SET last_cleaned_1m = ?
        WHERE symbol = ? AND timeframe = ? AND source = ?
        """,
        (timestamp, symbol, timeframe, source),
    )
    conn.commit()
    conn.close()


def save_liquidations(
    symbol: str,
    timeframe: str,
    records: List[Dict[str, Any]],
    source: str = DEFAULT_SOURCE_BINANCE,
) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    for rec in records:
        cursor.execute(
            """
            INSERT OR REPLACE INTO liquidations
            (symbol, exchange, source, timeframe, timestamp, long_volume, short_volume, total_volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                rec.get("exchange"),
                rec.get("source", source),
                timeframe,
                rec["timestamp"],
                rec.get("long_volume"),
                rec.get("short_volume"),
                rec.get("total_volume"),
                now,
            ),
        )
    conn.commit()
    conn.close()


def save_open_interest(
    symbol: str,
    timeframe: str,
    records: List[Dict[str, Any]],
    source: str = DEFAULT_SOURCE_BINANCE,
) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    for rec in records:
        cursor.execute(
            """
            INSERT OR REPLACE INTO open_interest
            (symbol, exchange, source, timeframe, timestamp, oi_value, oi_change_24h, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                rec.get("exchange"),
                rec.get("source", source),
                timeframe,
                rec["timestamp"],
                rec.get("oi_value"),
                rec.get("oi_change_24h"),
                now,
            ),
        )
    conn.commit()
    conn.close()


def save_instrument(
    symbol: str,
    exchange: str,
    tick_size: Optional[float] = None,
    min_qty: Optional[float] = None,
    avg_volume_24h: Optional[float] = None,
    atr: Optional[float] = None,
    commission_open: Optional[float] = None,
    commission_close: Optional[float] = None,
) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    cursor.execute(
        """
        INSERT INTO instruments (symbol, exchange, tick_size, min_qty, avg_volume_24h, atr,
            commission_open, commission_close, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, exchange) DO UPDATE SET
            tick_size = excluded.tick_size,
            min_qty = excluded.min_qty,
            avg_volume_24h = excluded.avg_volume_24h,
            atr = COALESCE(excluded.atr, instruments.atr),
            commission_open = excluded.commission_open,
            commission_close = excluded.commission_close,
            updated_at = excluded.updated_at
        """,
        (
            symbol,
            exchange,
            tick_size,
            min_qty,
            avg_volume_24h,
            atr,
            commission_open,
            commission_close,
            now,
        ),
    )
    conn.commit()
    conn.close()


def update_instrument_atr(symbol: str, exchange: str, atr: float) -> None:
    """Только ATR + updated_at (строка `instruments` должна уже существовать)."""
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    cursor.execute(
        """
        UPDATE instruments
        SET atr = ?, updated_at = ?
        WHERE symbol = ? AND exchange = ?
        """,
        (atr, now, symbol, exchange),
    )
    conn.commit()
    conn.close()


def get_instrument(symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM instruments WHERE symbol = ? AND exchange = ?", (symbol, exchange))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def clean_old_minute_data(retention_days: int) -> None:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = int(time.time()) - retention_days * 86400
    cursor.execute("DELETE FROM ohlcv WHERE timeframe = '1m' AND timestamp < ?", (cutoff,))
    conn.commit()
    conn.close()


def save_level_events(events: List[Dict[str, Any]]) -> int:
    if not events:
        return 0
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    now = int(time.time())
    for e in events:
        cursor.execute(
            """
            INSERT INTO level_events (
                event_id, stable_level_id, symbol, month_utc, level_type, layer, tier,
                level_price, volume_peak, duration_hours, atr_daily, dist_start_atr,
                touch_time, return_time, penetration_atr, rebound_pure_atr, rebound_after_return_atr,
                cluster_size, window_start, window_end, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                stable_level_id = excluded.stable_level_id,
                symbol = excluded.symbol,
                month_utc = excluded.month_utc,
                level_type = excluded.level_type,
                layer = excluded.layer,
                tier = excluded.tier,
                level_price = excluded.level_price,
                volume_peak = excluded.volume_peak,
                duration_hours = excluded.duration_hours,
                atr_daily = excluded.atr_daily,
                dist_start_atr = excluded.dist_start_atr,
                touch_time = excluded.touch_time,
                return_time = excluded.return_time,
                penetration_atr = excluded.penetration_atr,
                rebound_pure_atr = excluded.rebound_pure_atr,
                rebound_after_return_atr = excluded.rebound_after_return_atr,
                cluster_size = excluded.cluster_size,
                window_start = excluded.window_start,
                window_end = excluded.window_end,
                created_at = excluded.created_at
            """,
            (
                e.get("event_id"),
                e.get("stable_level_id"),
                e.get("symbol"),
                e.get("month_utc"),
                e.get("level_type"),
                e.get("layer"),
                e.get("tier"),
                e.get("level_price"),
                e.get("volume_peak"),
                e.get("duration_hours"),
                e.get("atr_daily"),
                e.get("dist_start_atr"),
                e.get("touch_time"),
                e.get("return_time"),
                e.get("penetration_atr"),
                e.get("rebound_pure_atr"),
                e.get("rebound_after_return_atr"),
                e.get("cluster_size"),
                e.get("window_start"),
                e.get("window_end"),
                now,
            ),
        )
    conn.commit()
    conn.close()
    return len(events)


def get_level_events_since(start_ts: int) -> List[Dict[str, Any]]:
    _ensure_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM level_events
        WHERE touch_time >= ?
        ORDER BY touch_time DESC, symbol ASC
        """,
        (int(start_ts),),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


class OHLCVRepository:
    def save_batch(self, symbol: str, timeframe: str, records: List[Dict[str, Any]]) -> None:
        save_ohlcv(symbol, timeframe, records)

    def get_last_timestamp(self, symbol: str, timeframe: str, source: str) -> Optional[int]:
        _ensure_db()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(timestamp) FROM ohlcv
            WHERE symbol = ? AND timeframe = ? AND ifnull(source, '') = ?
            """,
            (symbol, timeframe, source),
        )
        row = cursor.fetchone()
        conn.close()
        return int(row[0]) if row and row[0] is not None else None


class MetadataRepository:
    def get_last_updated(self, symbol: str, timeframe: str, source: str) -> Optional[int]:
        return get_last_update(symbol, timeframe, source)

    def update(
        self,
        symbol: str,
        timeframe: str,
        source: str,
        last_updated: int,
        last_full_update: Optional[int] = None,
    ) -> None:
        update_metadata(symbol, timeframe, last_updated, last_full_update, source=source)

    def set_last_cleaned(self, symbol: str, timeframe: str, source: str, timestamp: int) -> None:
        set_last_cleaned(symbol, timeframe, timestamp, source=source)


class InstrumentsRepository:
    def save_or_update(self, symbol: str, exchange: str, data: Dict[str, Any]) -> None:
        save_instrument(
            symbol,
            exchange,
            tick_size=data.get("tick_size"),
            min_qty=data.get("min_qty"),
            avg_volume_24h=data.get("avg_volume_24h"),
            atr=data.get("atr"),
            commission_open=data.get("commission_open"),
            commission_close=data.get("commission_close"),
        )

    def update_atr(self, symbol: str, exchange: str, atr: float) -> None:
        update_instrument_atr(symbol, exchange, atr)

    def get(self, symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
        return get_instrument(symbol, exchange)

    def get_symbols_with_min_avg_volume(
        self,
        *,
        min_avg_volume_24h: float,
        exchange: str = "bybit_futures",
    ) -> List[str]:
        """Возвращает символы, у которых avg_volume_24h >= порога."""
        _ensure_db()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT symbol
            FROM instruments
            WHERE exchange = ?
              AND ifnull(avg_volume_24h, 0) >= ?
            """,
            (exchange, float(min_avg_volume_24h)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [str(r["symbol"]) for r in rows]


class OIRepository:
    def save_batch(
        self,
        symbol: str,
        timeframe: str,
        records: List[Dict[str, Any]],
        source: str,
    ) -> None:
        save_open_interest(symbol=symbol, timeframe=timeframe, records=records, source=source)

    def get_last_timestamp(self, symbol: str, timeframe: str, source: str) -> Optional[int]:
        _ensure_db()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(timestamp) FROM open_interest
            WHERE symbol = ? AND timeframe = ? AND source = ?
            """,
            (symbol, timeframe, source),
        )
        row = cursor.fetchone()
        conn.close()
        return int(row[0]) if row and row[0] is not None else None


class LiquidationsRepository:
    def save_batch(
        self,
        symbol: str,
        timeframe: str,
        records: List[Dict[str, Any]],
        source: str,
    ) -> None:
        save_liquidations(symbol=symbol, timeframe=timeframe, records=records, source=source)

    def get_last_timestamp(self, symbol: str, timeframe: str, source: str) -> Optional[int]:
        _ensure_db()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(timestamp) FROM liquidations
            WHERE symbol = ? AND timeframe = ? AND source = ?
            """,
            (symbol, timeframe, source),
        )
        row = cursor.fetchone()
        conn.close()
        return int(row[0]) if row and row[0] is not None else None


class LevelEventsRepository:
    def save_batch(self, events: List[Dict[str, Any]]) -> int:
        return save_level_events(events)

    def get_since(self, start_ts: int) -> List[Dict[str, Any]]:
        return get_level_events_since(start_ts)
