from trading_bot.data.manual_global_hvn_db import (
    LEVEL_TYPE_MANUAL_GLOBAL_HVN,
    ManualGlobalHvnRowParsed,
    parse_manual_global_sheet_row,
    upsert_manual_global_hvn_level,
)
from trading_bot.data.manual_global_hvn_sheet_sync import default_manual_global_hvn_symbols
from trading_bot.data.db import get_connection
from trading_bot.tools.sheets_reader import worksheet_title_for_symbol


def test_worksheet_title_for_symbol():
    assert worksheet_title_for_symbol("BTC/USDT") == "BTC_USDT"
    assert worksheet_title_for_symbol("SP500") == "SP500"


def test_default_manual_global_hvn_symbols_includes_context_macro_and_indices():
    syms = default_manual_global_hvn_symbols()
    assert "BTC/USDT" in syms
    assert "ETH/BTC" in syms
    assert "SP500" in syms
    assert "TOTAL" in syms


def test_parse_manual_global_sheet_row_ok():
    r = parse_manual_global_sheet_row(
        {
            "stable_level_id": "BTC_USDT_001",
            "price": "65000.5",
            "tier": "1",
            "is_active": "1",
        }
    )
    assert r is not None
    assert r.stable_level_id == "BTC_USDT_001"
    assert r.price == 65000.5
    assert r.tier == "1"
    assert r.is_active is True


def test_parse_manual_global_sheet_row_inactive():
    r = parse_manual_global_sheet_row(
        {
            "stable_level_id": "A",
            "price": "1",
            "tier": "2",
            "is_active": "0",
        }
    )
    assert r is not None
    assert r.is_active is False


def test_parse_manual_global_sheet_row_skip_no_id():
    assert parse_manual_global_sheet_row({"price": "1", "tier": "1"}) is None


def test_parse_manual_global_sheet_row_skip_bad_tier():
    assert (
        parse_manual_global_sheet_row(
            {"stable_level_id": "a", "price": "1", "tier": "0", "is_active": "1"}
        )
        is None
    )


def test_upsert_manual_global_hvn_insert_and_update(clean_db):
    p = ManualGlobalHvnRowParsed(
        stable_level_id="TEST_LVL_1",
        price=100.0,
        tier="1",
        is_active=True,
    )
    assert upsert_manual_global_hvn_level(symbol="BTC/USDT", parsed=p) == "inserted"
    conn = get_connection()
    row = conn.execute(
        """
        SELECT symbol, price, tier, is_active, level_type, origin, layer, strength
        FROM price_levels WHERE stable_level_id = ?
        """,
        ("TEST_LVL_1",),
    ).fetchone()
    conn.close()
    assert row is not None
    assert row["symbol"] == "BTC/USDT"
    assert row["price"] == 100.0
    assert row["tier"] == "1"
    assert int(row["is_active"]) == 1
    assert row["level_type"] == LEVEL_TYPE_MANUAL_GLOBAL_HVN
    assert float(row["strength"]) == 0.0

    p2 = ManualGlobalHvnRowParsed(
        stable_level_id="TEST_LVL_1",
        price=101.0,
        tier="2",
        is_active=False,
    )
    assert upsert_manual_global_hvn_level(symbol="BTC/USDT", parsed=p2) == "updated"
    conn = get_connection()
    row2 = conn.execute(
        "SELECT price, tier, is_active FROM price_levels WHERE stable_level_id = ?",
        ("TEST_LVL_1",),
    ).fetchone()
    conn.close()
    assert float(row2["price"]) == 101.0
    assert row2["tier"] == "2"
    assert int(row2["is_active"]) == 0
