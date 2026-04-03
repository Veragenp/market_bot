from trading_bot.analytics.level_identity import round_to_tick, stable_level_id


def test_round_to_tick():
    assert round_to_tick(1.23456, 0.0001) == 1.2346


def test_stable_level_id_is_deterministic():
    a = stable_level_id(
        symbol="BTC/USDT",
        level_type="volume_profile_peaks",
        layer="L",
        tier="Tier 1 (Бетон)",
        price=70606.141,
        tick_size=0.1,
    )
    b = stable_level_id(
        symbol="BTC/USDT",
        level_type="volume_profile_peaks",
        layer="L",
        tier="Tier 1 (Бетон)",
        price=70606.149,
        tick_size=0.1,
    )
    assert a == b

