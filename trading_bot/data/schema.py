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
            event_status TEXT,
            pre_side TEXT,
            volume_peak REAL,
            duration_hours REAL,
            atr_daily REAL,
            atr_pct REAL,
            dist_start_atr REAL,
            touch_time INTEGER NOT NULL,
            return_time INTEGER,
            penetration_atr REAL,
            penetration_pct REAL,
            rebound_pure_atr REAL,
            rebound_pure_pct REAL,
            rebound_after_return_atr REAL,
            rebound_after_return_pct REAL,
            confirm_time INTEGER,
            confirm_time_sec INTEGER,
            touch_count_before_confirm INTEGER,
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cycle_id TEXT,
            structural_cycle_id TEXT,
            position_state TEXT NOT NULL DEFAULT 'none' CHECK (position_state IN ('none', 'long', 'short')),
            cycle_phase TEXT NOT NULL DEFAULT 'arming' CHECK (cycle_phase IN ('arming', 'in_position', 'closed')),
            levels_frozen INTEGER NOT NULL DEFAULT 0,
            cycle_version INTEGER NOT NULL DEFAULT 0,
            channel_mode TEXT NOT NULL DEFAULT 'two_sided' CHECK (channel_mode IN ('two_sided', 'single_sided')),
            known_side TEXT NOT NULL DEFAULT 'both' CHECK (known_side IN ('none', 'long', 'short', 'both')),
            need_rebuild_opposite INTEGER NOT NULL DEFAULT 0,
            opposite_rebuild_deadline_ts INTEGER,
            opposite_rebuild_attempts INTEGER NOT NULL DEFAULT 0,
            last_rebuild_reason TEXT,
            last_group_touch_event_ts INTEGER,
            last_group_touch_cycle_id TEXT,
            last_group_touch_source TEXT,
            last_group_touch_symbols_json TEXT,
            close_reason TEXT,
            last_package_exit_reason TEXT,
            last_transition_at INTEGER,
            updated_at INTEGER NOT NULL
        )
        """
    )
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "structural_cycle_id" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN structural_cycle_id TEXT")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "channel_mode" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN channel_mode TEXT NOT NULL DEFAULT 'two_sided'")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "known_side" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN known_side TEXT NOT NULL DEFAULT 'both'")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "need_rebuild_opposite" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN need_rebuild_opposite INTEGER NOT NULL DEFAULT 0")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "opposite_rebuild_deadline_ts" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN opposite_rebuild_deadline_ts INTEGER")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "opposite_rebuild_attempts" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN opposite_rebuild_attempts INTEGER NOT NULL DEFAULT 0")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "last_rebuild_reason" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN last_rebuild_reason TEXT")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "last_group_touch_event_ts" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN last_group_touch_event_ts INTEGER")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "last_group_touch_cycle_id" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN last_group_touch_cycle_id TEXT")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "last_group_touch_source" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN last_group_touch_source TEXT")
    _ts_cols = _column_names(cursor, "trading_state")
    if _ts_cols and "last_group_touch_symbols_json" not in _ts_cols:
        cursor.execute("ALTER TABLE trading_state ADD COLUMN last_group_touch_symbols_json TEXT")
    cursor.execute(
        """
        INSERT OR IGNORE INTO trading_state (id, cycle_id, structural_cycle_id, position_state, cycle_phase, levels_frozen, cycle_version, close_reason, last_transition_at, updated_at)
        VALUES (1, NULL, NULL, 'none', 'arming', 0, 0, NULL, NULL, strftime('%s','now'))
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cycle_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
            level_step INTEGER NOT NULL DEFAULT 1,
            level_price REAL NOT NULL,
            source_level_id INTEGER,
            tier TEXT,
            volume_peak REAL,
            distance_atr REAL,
            ref_price REAL,
            ref_price_source TEXT,
            ref_price_ts INTEGER,
            is_primary INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            frozen_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(cycle_id, symbol, direction, level_step),
            FOREIGN KEY (source_level_id) REFERENCES price_levels(id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cycle_levels_cycle_dir ON cycle_levels(cycle_id, direction, is_active)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cycle_levels_symbol_cycle ON cycle_levels(symbol, cycle_id)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cycle_level_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
            source_level_id INTEGER,
            level_price REAL NOT NULL,
            used_at INTEGER NOT NULL,
            reason TEXT,
            cycle_id TEXT,
            FOREIGN KEY (source_level_id) REFERENCES price_levels(id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cycle_usage_symbol_dir_time ON cycle_level_usage(symbol, direction, used_at DESC)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS structural_cycles (
            id TEXT PRIMARY KEY,
            phase TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            params_json TEXT,
            pool_median_w REAL,
            pool_mad REAL,
            pool_k REAL,
            symbols_valid_count INTEGER,
            touch_started_at INTEGER,
            entry_timer_until INTEGER,
            cancel_reason TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_structural_cycles_phase ON structural_cycles(phase, updated_at DESC)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS structural_cycle_symbols (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL,
            level_below_id INTEGER,
            level_above_id INTEGER,
            L_price REAL,
            U_price REAL,
            atr REAL,
            W_atr REAL,
            mid_price REAL,
            mid_band_low REAL,
            mid_band_high REAL,
            ref_price_ws REAL,
            evaluated_at INTEGER NOT NULL,
            tier_below TEXT,
            tier_above TEXT,
            volume_peak_below REAL,
            volume_peak_above REAL,
            FOREIGN KEY (level_below_id) REFERENCES price_levels(id),
            FOREIGN KEY (level_above_id) REFERENCES price_levels(id),
            FOREIGN KEY (cycle_id) REFERENCES structural_cycles(id),
            UNIQUE(cycle_id, symbol)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_structural_cycle_symbols_cycle ON structural_cycle_symbols(cycle_id)"
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS structural_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id TEXT NOT NULL,
            symbol TEXT,
            event_type TEXT NOT NULL,
            price REAL,
            ts INTEGER NOT NULL,
            meta_json TEXT,
            FOREIGN KEY (cycle_id) REFERENCES structural_cycles(id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_structural_events_cycle_ts ON structural_events(cycle_id, ts)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_structural_events_symbol_ts ON structural_events(symbol, ts)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_stage_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            cycle_id TEXT,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            severity TEXT,
            message TEXT,
            details_json TEXT,
            started_at INTEGER,
            finished_at INTEGER,
            duration_ms INTEGER,
            created_at INTEGER NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_stage_cycle_created ON ops_stage_runs(cycle_id, created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_stage_stage_created ON ops_stage_runs(stage, created_at DESC)"
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

    if current_version < 10:
        le_cols = _column_names(cursor, "level_events")
        def _safe_add(col: str, ddl: str) -> None:
            if col in le_cols:
                return
            try:
                cursor.execute(ddl)
            except Exception:
                # idempotent migration guard for partially applied local DBs
                pass

        _safe_add("event_status", "ALTER TABLE level_events ADD COLUMN event_status TEXT")
        _safe_add("pre_side", "ALTER TABLE level_events ADD COLUMN pre_side TEXT")
        _safe_add("atr_pct", "ALTER TABLE level_events ADD COLUMN atr_pct REAL")
        _safe_add("penetration_pct", "ALTER TABLE level_events ADD COLUMN penetration_pct REAL")
        _safe_add("rebound_pure_pct", "ALTER TABLE level_events ADD COLUMN rebound_pure_pct REAL")
        _safe_add("rebound_after_return_pct", "ALTER TABLE level_events ADD COLUMN rebound_after_return_pct REAL")
        _safe_add("confirm_time", "ALTER TABLE level_events ADD COLUMN confirm_time INTEGER")
        _safe_add("confirm_time_sec", "ALTER TABLE level_events ADD COLUMN confirm_time_sec INTEGER")
        _safe_add(
            "touch_count_before_confirm",
            "ALTER TABLE level_events ADD COLUMN touch_count_before_confirm INTEGER",
        )

        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (10, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 11:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cycle_id TEXT,
                position_state TEXT NOT NULL DEFAULT 'none' CHECK (position_state IN ('none', 'long', 'short')),
                cycle_phase TEXT NOT NULL DEFAULT 'arming' CHECK (cycle_phase IN ('arming', 'in_position', 'closed')),
                levels_frozen INTEGER NOT NULL DEFAULT 0,
                cycle_version INTEGER NOT NULL DEFAULT 0,
                close_reason TEXT,
                last_transition_at INTEGER,
                updated_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO trading_state (id, cycle_id, position_state, cycle_phase, levels_frozen, cycle_version, close_reason, last_transition_at, updated_at)
            VALUES (1, NULL, 'none', 'arming', 0, 0, NULL, NULL, strftime('%s','now'))
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cycle_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
                level_step INTEGER NOT NULL DEFAULT 1,
                level_price REAL NOT NULL,
                source_level_id INTEGER,
                tier TEXT,
                volume_peak REAL,
                distance_atr REAL,
                ref_price REAL,
                ref_price_source TEXT,
                ref_price_ts INTEGER,
                is_primary INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                frozen_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(cycle_id, symbol, direction, level_step),
                FOREIGN KEY (source_level_id) REFERENCES price_levels(id)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cycle_levels_cycle_dir ON cycle_levels(cycle_id, direction, is_active)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cycle_levels_symbol_cycle ON cycle_levels(symbol, cycle_id)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cycle_level_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
                source_level_id INTEGER,
                level_price REAL NOT NULL,
                used_at INTEGER NOT NULL,
                reason TEXT,
                cycle_id TEXT,
                FOREIGN KEY (source_level_id) REFERENCES price_levels(id)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cycle_usage_symbol_dir_time ON cycle_level_usage(symbol, direction, used_at DESC)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (11, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 12:
        cl_cols = _column_names(cursor, "cycle_levels")
        if "ref_price" not in cl_cols:
            cursor.execute("ALTER TABLE cycle_levels ADD COLUMN ref_price REAL")
        if "ref_price_source" not in cl_cols:
            cursor.execute("ALTER TABLE cycle_levels ADD COLUMN ref_price_source TEXT")
        if "ref_price_ts" not in cl_cols:
            cursor.execute("ALTER TABLE cycle_levels ADD COLUMN ref_price_ts INTEGER")
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (12, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 13:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS structural_cycles (
                id TEXT PRIMARY KEY,
                phase TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                params_json TEXT,
                pool_median_w REAL,
                pool_mad REAL,
                pool_k REAL,
                symbols_valid_count INTEGER,
                touch_started_at INTEGER,
                entry_timer_until INTEGER,
                cancel_reason TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_structural_cycles_phase ON structural_cycles(phase, updated_at DESC)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS structural_cycle_symbols (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                level_below_id INTEGER,
                level_above_id INTEGER,
                L_price REAL,
                U_price REAL,
                atr REAL,
                W_atr REAL,
                mid_price REAL,
                mid_band_low REAL,
                mid_band_high REAL,
                ref_price_ws REAL,
                evaluated_at INTEGER NOT NULL,
                tier_below TEXT,
                tier_above TEXT,
                volume_peak_below REAL,
                volume_peak_above REAL,
                FOREIGN KEY (level_below_id) REFERENCES price_levels(id),
                FOREIGN KEY (level_above_id) REFERENCES price_levels(id),
                FOREIGN KEY (cycle_id) REFERENCES structural_cycles(id),
                UNIQUE(cycle_id, symbol)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_structural_cycle_symbols_cycle ON structural_cycle_symbols(cycle_id)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS structural_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                symbol TEXT,
                event_type TEXT NOT NULL,
                price REAL,
                ts INTEGER NOT NULL,
                meta_json TEXT,
                FOREIGN KEY (cycle_id) REFERENCES structural_cycles(id)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_structural_events_cycle_ts ON structural_events(cycle_id, ts)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_structural_events_symbol_ts ON structural_events(symbol, ts)"
        )
        ts_cols = _column_names(cursor, "trading_state")
        if ts_cols and "structural_cycle_id" not in ts_cols:
            cursor.execute("ALTER TABLE trading_state ADD COLUMN structural_cycle_id TEXT")
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (13, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 14:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_detector_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                cycle_id TEXT NOT NULL,
                structural_cycle_id TEXT,
                symbol TEXT NOT NULL,
                event_type TEXT NOT NULL,
                price REAL,
                long_level_price REAL,
                short_level_price REAL,
                atr_used REAL,
                distance_to_long_atr REAL,
                distance_to_short_atr REAL,
                meta_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_det_cycle_ts ON entry_detector_events(cycle_id, ts DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_det_symbol_ts ON entry_detector_events(symbol, ts DESC)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS exec_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                cycle_id TEXT,
                structural_cycle_id TEXT,
                position_record_id INTEGER,
                order_role TEXT,
                parent_exec_order_id INTEGER,
                client_order_id TEXT,
                bybit_order_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                qty REAL NOT NULL,
                price REAL,
                status TEXT NOT NULL,
                exchange_status TEXT,
                filled_qty REAL,
                avg_fill_price REAL,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                raw_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_orders_client ON exec_orders(client_order_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_orders_bybit ON exec_orders(bybit_order_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_orders_pos ON exec_orders(position_record_id)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS position_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                cycle_id TEXT,
                structural_cycle_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('long', 'short')),
                status TEXT NOT NULL CHECK (status IN ('pending', 'open', 'closed', 'cancelled')),
                qty REAL NOT NULL,
                entry_price REAL,
                entry_price_fact REAL,
                exit_price REAL,
                exit_price_fact REAL,
                filled_qty REAL,
                entry_exec_order_id INTEGER,
                stop_exec_order_id INTEGER,
                exit_exec_order_id INTEGER,
                opened_at INTEGER,
                closed_at INTEGER,
                realized_pnl REAL,
                fees REAL,
                close_reason TEXT,
                notes TEXT,
                meta_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_records_symbol ON position_records(symbol, updated_at DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_records_status ON position_records(status)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_statistics_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                scope TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_stats_period ON trade_statistics_snapshots(period_end DESC)"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS exec_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exec_order_id INTEGER,
                position_record_id INTEGER,
                cycle_id TEXT,
                structural_cycle_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT,
                trade_id TEXT,
                fill_price REAL,
                fill_qty REAL,
                fee REAL,
                fee_currency TEXT,
                ts INTEGER NOT NULL,
                raw_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_fills_order_ts ON exec_fills(exec_order_id, ts DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_fills_cycle_ts ON exec_fills(cycle_id, ts DESC)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (14, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 15:
        _ts15 = _column_names(cursor, "trading_state")
        if _ts15 and "allow_long_entry" not in _ts15:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN allow_long_entry INTEGER NOT NULL DEFAULT 1"
            )
        _ts15 = _column_names(cursor, "trading_state")
        if _ts15 and "allow_short_entry" not in _ts15:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN allow_short_entry INTEGER NOT NULL DEFAULT 1"
            )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_gate_confirmations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                cycle_id TEXT NOT NULL,
                structural_cycle_id TEXT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
                level_price REAL NOT NULL,
                entry_price REAL NOT NULL,
                atr REAL,
                long_atr_threshold_pct REAL,
                short_atr_threshold_pct REAL,
                meta_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_gate_conf_cycle ON entry_gate_confirmations(cycle_id, ts DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_gate_conf_sym ON entry_gate_confirmations(symbol, ts DESC)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (15, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 16:
        _pr16 = _column_names(cursor, "position_records")
        if _pr16 and "entry_gate_confirmation_id" not in _pr16:
            cursor.execute(
                "ALTER TABLE position_records ADD COLUMN entry_gate_confirmation_id INTEGER"
            )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_records_gate_conf "
            "ON position_records(entry_gate_confirmation_id)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (16, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 17:
        _ts17 = _column_names(cursor, "trading_state")
        if _ts17 and "last_group_touch_event_ts" not in _ts17:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_group_touch_event_ts INTEGER"
            )
        _ts17 = _column_names(cursor, "trading_state")
        if _ts17 and "last_group_touch_cycle_id" not in _ts17:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_group_touch_cycle_id TEXT"
            )
        _ts17 = _column_names(cursor, "trading_state")
        if _ts17 and "last_group_touch_source" not in _ts17:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_group_touch_source TEXT"
            )
        _ts17 = _column_names(cursor, "trading_state")
        if _ts17 and "last_group_touch_symbols_json" not in _ts17:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_group_touch_symbols_json TEXT"
            )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (17, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 18:
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "channel_mode" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN channel_mode TEXT NOT NULL DEFAULT 'two_sided'"
            )
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "known_side" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN known_side TEXT NOT NULL DEFAULT 'both'"
            )
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "need_rebuild_opposite" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN need_rebuild_opposite INTEGER NOT NULL DEFAULT 0"
            )
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "opposite_rebuild_deadline_ts" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN opposite_rebuild_deadline_ts INTEGER"
            )
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "opposite_rebuild_attempts" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN opposite_rebuild_attempts INTEGER NOT NULL DEFAULT 0"
            )
        _ts18 = _column_names(cursor, "trading_state")
        if _ts18 and "last_rebuild_reason" not in _ts18:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_rebuild_reason TEXT"
            )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (18, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 19:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_stage_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                cycle_id TEXT,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                severity TEXT,
                message TEXT,
                details_json TEXT,
                started_at INTEGER,
                finished_at INTEGER,
                duration_ms INTEGER,
                created_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ops_stage_cycle_created ON ops_stage_runs(cycle_id, created_at DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ops_stage_stage_created ON ops_stage_runs(stage, created_at DESC)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (19, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 20:
        _eo20 = _column_names(cursor, "exec_orders")
        if _eo20 and "order_role" not in _eo20:
            cursor.execute("ALTER TABLE exec_orders ADD COLUMN order_role TEXT")
        _eo20 = _column_names(cursor, "exec_orders")
        if _eo20 and "parent_exec_order_id" not in _eo20:
            cursor.execute("ALTER TABLE exec_orders ADD COLUMN parent_exec_order_id INTEGER")
        _eo20 = _column_names(cursor, "exec_orders")
        if _eo20 and "exchange_status" not in _eo20:
            cursor.execute("ALTER TABLE exec_orders ADD COLUMN exchange_status TEXT")
        _eo20 = _column_names(cursor, "exec_orders")
        if _eo20 and "filled_qty" not in _eo20:
            cursor.execute("ALTER TABLE exec_orders ADD COLUMN filled_qty REAL")
        _eo20 = _column_names(cursor, "exec_orders")
        if _eo20 and "avg_fill_price" not in _eo20:
            cursor.execute("ALTER TABLE exec_orders ADD COLUMN avg_fill_price REAL")

        _pr20 = _column_names(cursor, "position_records")
        if _pr20 and "entry_price_fact" not in _pr20:
            cursor.execute("ALTER TABLE position_records ADD COLUMN entry_price_fact REAL")
        _pr20 = _column_names(cursor, "position_records")
        if _pr20 and "exit_price_fact" not in _pr20:
            cursor.execute("ALTER TABLE position_records ADD COLUMN exit_price_fact REAL")
        _pr20 = _column_names(cursor, "position_records")
        if _pr20 and "filled_qty" not in _pr20:
            cursor.execute("ALTER TABLE position_records ADD COLUMN filled_qty REAL")
        _pr20 = _column_names(cursor, "position_records")
        if _pr20 and "stop_exec_order_id" not in _pr20:
            cursor.execute("ALTER TABLE position_records ADD COLUMN stop_exec_order_id INTEGER")
        _pr20 = _column_names(cursor, "position_records")
        if _pr20 and "close_reason" not in _pr20:
            cursor.execute("ALTER TABLE position_records ADD COLUMN close_reason TEXT")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS exec_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exec_order_id INTEGER,
                position_record_id INTEGER,
                cycle_id TEXT,
                structural_cycle_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT,
                trade_id TEXT,
                fill_price REAL,
                fill_qty REAL,
                fee REAL,
                fee_currency TEXT,
                ts INTEGER NOT NULL,
                raw_json TEXT
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_fills_order_ts ON exec_fills(exec_order_id, ts DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_fills_cycle_ts ON exec_fills(cycle_id, ts DESC)"
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (20, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 21:
        _ts21 = _column_names(cursor, "trading_state")
        if _ts21 and "last_package_exit_reason" not in _ts21:
            cursor.execute(
                "ALTER TABLE trading_state ADD COLUMN last_package_exit_reason TEXT"
            )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (21, ?)",
            (int(time.time()),),
        )
        conn.commit()

    if current_version < 22:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sheet_stats_exported_position (
                position_record_id INTEGER PRIMARY KEY,
                exported_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (22, ?)",
            (int(time.time()),),
        )
        conn.commit()

    # Migration v23: Session tracking + Position sync fields
    if current_version < 23:
        # position_records: sync fields
        _pr23 = _column_names(cursor, "position_records")
        if _pr23 and "exchange_position_id" not in _pr23:
            cursor.execute("ALTER TABLE position_records ADD COLUMN exchange_position_id TEXT")
        _pr23 = _column_names(cursor, "position_records")
        if _pr23 and "last_sync_ts" not in _pr23:
            cursor.execute("ALTER TABLE position_records ADD COLUMN last_sync_ts INTEGER")
        _pr23 = _column_names(cursor, "position_records")
        if _pr23 and "sync_status" not in _pr23:
            cursor.execute("ALTER TABLE position_records ADD COLUMN sync_status TEXT DEFAULT 'pending'")
        _pr23 = _column_names(cursor, "position_records")
        if _pr23 and "last_sync_error" not in _pr23:
            cursor.execute("ALTER TABLE position_records ADD COLUMN last_sync_error TEXT")
        
        # trading_state: session tracking
        _ts23 = _column_names(cursor, "trading_state")
        if _ts23 and "last_session_id" not in _ts23:
            cursor.execute("ALTER TABLE trading_state ADD COLUMN last_session_id TEXT")
        _ts23 = _column_names(cursor, "trading_state")
        if _ts23 and "last_start_ts" not in _ts23:
            cursor.execute("ALTER TABLE trading_state ADD COLUMN last_start_ts INTEGER")
        _ts23 = _column_names(cursor, "trading_state")
        if _ts23 and "last_start_mode" not in _ts23:
            cursor.execute("ALTER TABLE trading_state ADD COLUMN last_start_mode TEXT")
        _ts23 = _column_names(cursor, "trading_state")
        if _ts23 and "opposite_rebuild_in_progress" not in _ts23:
            cursor.execute("ALTER TABLE trading_state ADD COLUMN opposite_rebuild_in_progress INTEGER DEFAULT 0")
        
        # runtime_state table for temporary flags
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER
            )
        """)
        
        cursor.execute(
            "INSERT INTO db_version (version, applied_at) VALUES (23, ?)",
            (int(time.time()),),
        )
        conn.commit()

    conn.close()

