from __future__ import annotations

from trading_bot.tools import bybit_trading as bt


class _FakeSession:
    def __init__(self) -> None:
        self.calls = []

    def get_positions(self, **kwargs):
        self.calls.append(kwargs)
        return {"result": {"list": []}}


def test_get_linear_positions_uses_settle_coin_when_symbol_missing(monkeypatch):
    sess = _FakeSession()
    monkeypatch.setattr(bt, "_session", lambda: sess)

    out = bt.get_linear_positions()

    assert out == {"result": {"list": []}}
    assert len(sess.calls) == 1
    assert sess.calls[0].get("category") == "linear"
    assert sess.calls[0].get("settleCoin") == "USDT"
    assert "symbol" not in sess.calls[0]


def test_get_linear_positions_uses_symbol_when_passed(monkeypatch):
    sess = _FakeSession()
    monkeypatch.setattr(bt, "_session", lambda: sess)

    out = bt.get_linear_positions("AAVE/USDT")

    assert out == {"result": {"list": []}}
    assert len(sess.calls) == 1
    assert sess.calls[0].get("category") == "linear"
    assert sess.calls[0].get("symbol") == "AAVEUSDT"
    assert "settleCoin" not in sess.calls[0]
