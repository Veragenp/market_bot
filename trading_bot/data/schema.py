from __future__ import annotations

import time
import uuid

from trading_bot.data.db import get_connection


def _column_names(cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def init_db() -> None:
    """Создаёт таблицы и индексы текущей схемы (v5+)."""
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
            atr REAL,
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS price_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            level_type TEXT NOT NULL,
            layer TEXT,
            strength REAL DEFAULT 0.0,
            volume_peak REAL,
            duration_hours REAL,
            t_start_unix INTEGER,
            t_end_unix INTEGER,
            touch_count INTEGER DEFAULT 0,
            last_touch INTEGER,
            low_volume_zone_above INTEGER DEFAULT 0,
            low_volume_zone_below INTEGER DEFAULT 0,
            tier TEXT,
            created_at INTEGER NOT NULL,
            expires_at INTEGER,
            is_active INTEGER DEFAULT 1,
            stable_level_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            origin TEXT NOT NULL DEFAULT 'auto',
            timeframe TEXT,
            parent_stable_level_id TEXT,
            confirmed_human_at INTEGER,
            updated_at INTEGER,
            last_matched_calc_at INTEGER,
            lookback_days INTEGER
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_symbol_active ON price_levels(symbol, is_active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_symbol_layer ON price_levels(symbol, layer)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_strength ON price_levels(symbol, strength DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_tier ON price_levels(symbol, tier)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pl_symbol_type_status ON price_levels(symbol, level_type, status)"
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_pl_stable_level_id ON price_levels(stable_level_id)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS level_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            stable_level_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            month_utc TEXT,
            level_type TEXT,
            layer TEXT,
            tier TEXT,
            level_price REAL NOT NULL,
            volume_peak REAL,
            duration_hours REAL,
            atr_daily REAL,
            dist_start_atr REAL,
            touch_time INTEGER NOT NULL,
            return_time INTEGER,
            penetration_atr REAL,
            rebound_pure_atr REAL,
            rebound_after_return_atr REAL,
            cluster_size INTEGER,
            window_start INTEGER,
            window_end INTEGER,
            created_at INTEGER NOT NULL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_touch ON level_events(touch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_symbol_touch ON level_events(symbol, touch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_stable ON level_events(stable_level_id)")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            btc_trend_1d TEXT,
            btc_trend_4h TEXT,
            btc_trend_1h TEXT,
            market_strength REAL,
            fear_greed_index INTEGER,
            sp500_trend TEXT,
            dxy_trend TEXT,
            dominance_btc REAL,
            others_target TEXT,
            others_confidence REAL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mc_timestamp ON market_context(timestamp DESC)")

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
                        WHEN symbol IN ('TOTAL', 'TOTAL2', 'TOTAL3', 'BTCD', 'OTHERSD', 'OTHERS') THEN 'tradingview'
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

    if current_version < 5:
        # --- new analytics tables for cluster entry ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS price_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                level_type TEXT NOT NULL,
                layer TEXT,
                strength REAL DEFAULT 0.0,
                volume_peak REAL,
                duration_hours REAL,
                t_start_unix INTEGER,
                t_end_unix INTEGER,
                touch_count INTEGER DEFAULT 0,
                last_touch INTEGER,
                low_volume_zone_above INTEGER DEFAULT 0,
                low_volume_zone_below INTEGER DEFAULT 0,
                tier TEXT,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                is_active INTEGER DEFAULT 1
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_symbol_active ON price_levels(symbol, is_active)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_symbol_layer ON price_levels(symbol, layer)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_strength ON price_levels(symbol, strength DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pl_tier ON price_levels(symbol, tier)")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS market_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                btc_trend_1d TEXT,
                btc_trend_4h TEXT,
                btc_trend_1h TEXT,
                market_strength REAL,
                fear_greed_index INTEGER,
                sp500_trend TEXT,
                dxy_trend TEXT,
                dominance_btc REAL,
                others_target TEXT,
                others_confidence REAL
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mc_timestamp ON market_context(timestamp DESC)")

        # --- trade_levels: add link to analytical level + keep compatibility ---
        cursor.execute("PRAGMA foreign_keys = OFF")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_levels_v5 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
                entry_price REAL NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                price_level_id INTEGER,
                generated_at INTEGER NOT NULL,
                expires_at INTEGER,
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'expired', 'cancelled', 'hit', 'filled')),
                source TEXT DEFAULT 'agent',
                position_id INTEGER,
                FOREIGN KEY (price_level_id) REFERENCES price_levels(id)
            )
            """
        )

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_levels'")
        if cursor.fetchone() is not None:
            cursor.execute(
                """
                INSERT INTO trade_levels_v5 (
                    id, symbol, direction, entry_price, stop_loss, take_profit,
                    price_level_id, generated_at, expires_at, status, source, position_id
                )
                SELECT
                    id, symbol, direction, entry_price, stop_loss, take_profit,
                    NULL, generated_at, expires_at, status, source, NULL
                FROM trade_levels
                """
            )
            cursor.execute("DROP TABLE IF EXISTS trade_levels")

        cursor.execute("ALTER TABLE trade_levels_v5 RENAME TO trade_levels")
        cursor.execute("PRAGMA foreign_keys = ON")

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tl_symbol_status ON trade_levels(symbol, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tl_status_gen ON trade_levels(status, generated_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tl_price_level ON trade_levels(price_level_id)")

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (5, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 6:
        # --- price_levels: add timing columns for volume-profile zones ---
        pl_cols = _column_names(cursor, "price_levels")
        if "duration_hours" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN duration_hours REAL")
        if "t_start_unix" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN t_start_unix INTEGER")
        if "t_end_unix" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN t_end_unix INTEGER")

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (6, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 7:
        inst_cols = _column_names(cursor, "instruments")
        if "atr" not in inst_cols:
            cursor.execute("ALTER TABLE instruments ADD COLUMN atr REAL")

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (7, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 8:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS level_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                stable_level_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                month_utc TEXT,
                level_type TEXT,
                layer TEXT,
                tier TEXT,
                level_price REAL NOT NULL,
                volume_peak REAL,
                duration_hours REAL,
                atr_daily REAL,
                dist_start_atr REAL,
                touch_time INTEGER NOT NULL,
                return_time INTEGER,
                penetration_atr REAL,
                rebound_pure_atr REAL,
                rebound_after_return_atr REAL,
                cluster_size INTEGER,
                window_start INTEGER,
                window_end INTEGER,
                created_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_touch ON level_events(touch_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_symbol_touch ON level_events(symbol, touch_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_le_stable ON level_events(stable_level_id)")
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (8, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 9:
        pl_cols = _column_names(cursor, "price_levels")
        if "stable_level_id" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN stable_level_id TEXT")
        if "status" not in pl_cols:
            cursor.execute(
                "ALTER TABLE price_levels ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
            )
        if "origin" not in pl_cols:
            cursor.execute(
                "ALTER TABLE price_levels ADD COLUMN origin TEXT NOT NULL DEFAULT 'auto'"
            )
        if "timeframe" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN timeframe TEXT")
        if "parent_stable_level_id" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN parent_stable_level_id TEXT")
        if "confirmed_human_at" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN confirmed_human_at INTEGER")
        if "updated_at" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN updated_at INTEGER")
        if "last_matched_calc_at" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN last_matched_calc_at INTEGER")
        if "lookback_days" not in pl_cols:
            cursor.execute("ALTER TABLE price_levels ADD COLUMN lookback_days INTEGER")

        cursor.execute(
            "UPDATE price_levels SET status = CASE WHEN is_active = 1 THEN 'active' ELSE 'archived' END"
        )
        cursor.execute(
            "UPDATE price_levels SET updated_at = created_at WHERE updated_at IS NULL"
        )

        cursor.execute("SELECT id FROM price_levels WHERE stable_level_id IS NULL OR stable_level_id = ''")
        for row in cursor.fetchall():
            cursor.execute(
                "UPDATE price_levels SET stable_level_id = ? WHERE id = ?",
                (str(uuid.uuid4()), int(row["id"])),
            )

        cursor.execute(
            "UPDATE price_levels SET level_type = 'vp_local' WHERE level_type = 'volume_profile_peaks'"
        )
        cursor.execute(
            "UPDATE price_levels SET level_type = 'vp_global' WHERE level_type = 'volume_profile_htf'"
        )
        cursor.execute(
            "UPDATE price_levels SET level_type = 'vp_global_4h_90d' "
            "WHERE level_type = 'volume_profile_htf_4h_90d'"
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pl_symbol_type_status ON price_levels(symbol, level_type, status)"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_pl_stable_level_id ON price_levels(stable_level_id)"
        )

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (9, ?)",
            (int(time.time()),),
        )
        conn.commit()

    conn.close()

