from __future__ import annotations

import time

from trading_bot.data.db import get_connection


def _column_names(cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def init_db() -> None:
    """Создаёт таблицы и индексы текущей схемы (v4+)."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlcv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL CHECK (timeframe IN ('1m', '1h', '4h', '1d', '1w', '1W', '1M')),
            timestamp INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source TEXT,
            extra TEXT,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cursor.execute("DROP INDEX IF EXISTS idx_ohlcv_unique")
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_unique
        ON ohlcv(symbol, timeframe, timestamp, ifnull(source, ''))
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_timeframe_timestamp
        ON ohlcv(symbol, timeframe, timestamp)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ohlcv_timeframe_timestamp
        ON ohlcv(timeframe, timestamp)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ohlcv_source
        ON ohlcv(source)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT,
            source TEXT NOT NULL DEFAULT 'binance',
            timeframe TEXT NOT NULL CHECK (timeframe IN ('1m', '1h', '4h', '1d', '1w', '1W', '1M')),
            timestamp INTEGER NOT NULL,
            long_volume REAL,
            short_volume REAL,
            total_volume REAL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_liquidations_symbol_timeframe_timestamp
        ON liquidations(symbol, timeframe, timestamp)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_liquidations_source
        ON liquidations(source)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS open_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT,
            source TEXT NOT NULL DEFAULT 'binance',
            timeframe TEXT NOT NULL CHECK (timeframe IN ('1m', '1h', '4h', '1d', '1w', '1W', '1M')),
            timestamp INTEGER NOT NULL,
            oi_value REAL,
            oi_change_24h REAL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_oi_symbol_timeframe_timestamp
        ON open_interest(symbol, timeframe, timestamp)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_oi_source
        ON open_interest(source)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL CHECK (timeframe IN ('1m', '1h', '4h', '1d', '1w', '1W', '1M')),
            source TEXT NOT NULL,
            last_updated INTEGER NOT NULL,
            last_full_update INTEGER,
            last_cleaned_1m INTEGER,
            PRIMARY KEY (symbol, timeframe, source)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metadata_source
        ON metadata(source)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            tick_size REAL,
            min_qty REAL,
            avg_volume_24h REAL,
            commission_open REAL,
            commission_close REAL,
            updated_at INTEGER,
            PRIMARY KEY (symbol, exchange)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_instruments_exchange
        ON instruments(exchange)
        """
    )

    conn.commit()
    conn.close()


def run_migrations() -> None:
    """Применяет миграции, если таблица версий не существует или версия ниже."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS db_version (
            version INTEGER PRIMARY KEY,
            applied_at INTEGER
        )
        """
    )
    cursor.execute("SELECT version FROM db_version ORDER BY version DESC LIMIT 1")
    row = cursor.fetchone()
    current_version = row[0] if row else 0

    if current_version < 1:
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (1, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 3:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
                entry_price REAL NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                generated_at INTEGER NOT NULL,
                expires_at INTEGER,
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'expired', 'cancelled', 'hit')),
                source TEXT DEFAULT 'agent'
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_levels_symbol ON trade_levels(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_levels_status ON trade_levels(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_levels_generated_at ON trade_levels(generated_at)")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS level_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level_id INTEGER NOT NULL,
                hit_time INTEGER NOT NULL,
                price REAL NOT NULL,
                FOREIGN KEY (level_id) REFERENCES trade_levels(id) ON DELETE CASCADE
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_level_hits_level_id ON level_hits(level_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_level_hits_hit_time ON level_hits(hit_time)")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_time INTEGER NOT NULL,
                direction TEXT NOT NULL,
                level_ids TEXT,
                position_id INTEGER,
                status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'executed', 'failed'))
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_decisions_decision_time ON trade_decisions(decision_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_decisions_status ON trade_decisions(status)")

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (3, ?)",
            (int(time.time()),),
        )
        conn.commit()
        current_version = 3

    if current_version < 4:
        # --- liquidations / open_interest: source ---
        liq_cols = _column_names(cursor, "liquidations")
        if "source" not in liq_cols:
            cursor.execute("ALTER TABLE liquidations ADD COLUMN source TEXT NOT NULL DEFAULT 'binance'")
            cursor.execute("UPDATE liquidations SET source = 'binance' WHERE source IS NULL")

        oi_cols = _column_names(cursor, "open_interest")
        if "source" not in oi_cols:
            cursor.execute("ALTER TABLE open_interest ADD COLUMN source TEXT NOT NULL DEFAULT 'binance'")
            cursor.execute("UPDATE open_interest SET source = 'binance' WHERE source IS NULL")

        # --- ohlcv: уникальность с учётом source ---
        cursor.execute("UPDATE ohlcv SET source = 'unknown' WHERE source IS NULL OR source = ''")
        cursor.execute("DROP INDEX IF EXISTS idx_ohlcv_unique")
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_unique
            ON ohlcv(symbol, timeframe, timestamp, ifnull(source, ''))
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_source ON ohlcv(source)")

        # --- metadata: составной PK с source ---
        meta_cols = _column_names(cursor, "metadata")
        if "source" not in meta_cols:
            cursor.execute(
                """
                CREATE TABLE metadata_mig (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    source TEXT NOT NULL,
                    last_updated INTEGER NOT NULL,
                    last_full_update INTEGER,
                    last_cleaned_1m INTEGER,
                    PRIMARY KEY (symbol, timeframe, source)
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO metadata_mig (symbol, timeframe, source, last_updated, last_full_update, last_cleaned_1m)
                SELECT
                    symbol,
                    timeframe,
                    CASE
                        WHEN symbol IN ('SP500', 'RTY', 'GOLD', 'DXY') THEN 'yfinance'
                        WHEN symbol IN ('TOTAL', 'TOTAL2', 'TOTAL3', 'BTCD', 'OTHERSD') THEN 'tradingview'
                        ELSE 'binance'
                    END,
                    last_updated,
                    last_full_update,
                    last_cleaned_1m
                FROM metadata
                """
            )
            cursor.execute("DROP TABLE metadata")
            cursor.execute("ALTER TABLE metadata_mig RENAME TO metadata")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_metadata_source ON metadata(source)")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS instruments (
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                tick_size REAL,
                min_qty REAL,
                avg_volume_24h REAL,
                commission_open REAL,
                commission_close REAL,
                updated_at INTEGER,
                PRIMARY KEY (symbol, exchange)
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_instruments_exchange ON instruments(exchange)")

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (4, ?)",
            (int(time.time()),),
        )
        conn.commit()

    conn.close()

