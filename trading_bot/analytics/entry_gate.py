"""
Второй слой после level_cross_monitor: логика tutorial_v3/trade_signal_processor.py.

ATR-порог к уровню, фиксация «входа» в SQLite (`entry_gate_confirmations`) вместо колонок E/F в Sheets.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from trading_bot.config import settings as st
from trading_bot.analytics.structural_cycle import rebuild_opposite_zone_on_cursor
from trading_bot.data.cycle_levels_db import (
    backfill_missing_cycle_side,
    export_cycle_levels_sheets_snapshot,
)
from trading_bot.analytics.level_cross_monitor import load_cycle_level_pairs
from trading_bot.tools.bybit_trading import to_bybit_symbol
from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier

if TYPE_CHECKING:
    from trading_bot.analytics.level_cross_monitor import LevelCrossMonitor

logger = logging.getLogger(__name__)


def _get_atr(cur, symbol_trade: str) -> Optional[float]:
    row = cur.execute(
        """
        SELECT atr FROM instruments
        WHERE symbol = ? AND exchange = 'bybit_futures'
        """,
        (symbol_trade,),
    ).fetchone()
    if not row or row["atr"] is None:
        return None
    v = float(row["atr"])
    return v if v > 0 else None


def _log_v3_event(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    event_type: str,
    symbol: Optional[str],
    price: Optional[float],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO entry_detector_events (
            ts, cycle_id, structural_cycle_id, symbol, event_type,
            price, long_level_price, short_level_price, atr_used,
            distance_to_long_atr, distance_to_short_atr, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?)
        """,
        (
            ts,
            cycle_id,
            structural_cycle_id,
            symbol or "",
            event_type,
            price,
            json.dumps(meta, ensure_ascii=False) if meta else None,
        ),
    )


def _load_cycle_side_symbols(cur, *, cycle_id: str, direction: str, symbols: List[str]) -> List[str]:
    if not symbols:
        return []
    ph = ",".join("?" * len(symbols))
    rows = cur.execute(
        f"""
        SELECT DISTINCT symbol
        FROM cycle_levels
        WHERE cycle_id = ?
          AND direction = ?
          AND level_step = 1
          AND is_active = 1
          AND symbol IN ({ph})
        """,
        (cycle_id, direction, *symbols),
    ).fetchall()
    return [str(r["symbol"]) for r in rows]


def _load_cycle_side_levels(cur, *, cycle_id: str, direction: str) -> Dict[str, float]:
    rows = cur.execute(
        """
        SELECT symbol, level_price
        FROM cycle_levels
        WHERE cycle_id = ? AND direction = ? AND level_step = 1 AND is_active = 1
        """,
        (cycle_id, direction),
    ).fetchall()
    return {str(r["symbol"]): float(r["level_price"]) for r in rows}


def _load_cycle_member_symbols(cur, *, cycle_id: str) -> List[str]:
    rows = cur.execute(
        """
        SELECT DISTINCT symbol
        FROM structural_cycle_symbols
        WHERE cycle_id = ? AND status = 'ok'
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    syms = [str(r["symbol"]) for r in rows]
    if syms:
        return syms
    # Backward/edge fallback: if no structural snapshot is present, use active cycle_levels symbols.
    rows = cur.execute(
        """
        SELECT DISTINCT symbol
        FROM cycle_levels
        WHERE cycle_id = ? AND is_active = 1
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    return [str(r["symbol"]) for r in rows]


def _ref_price_for_symbol(cur, *, symbol: str, prices: Dict[str, float]) -> Optional[float]:
    px = prices.get(symbol)
    if px is not None and float(px) > 0:
        return float(px)
    row = cur.execute(
        """
        SELECT close
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if not row or row["close"] is None:
        return None
    v = float(row["close"])
    return v if v > 0 else None


def signal_structural_ready(
    cur,
    *,
    structural_cycle_id: str,
    direction: str = "both",
) -> None:
    """Сигнал после группового условия structural (N в mid / переходы); один раз на цикл до cooldown."""
    row = cur.execute("SELECT cycle_id FROM trading_state WHERE id = 1").fetchone()
    cid = str(row["cycle_id"]) if row and row["cycle_id"] else structural_cycle_id
    _log_v3_event(
        cur,
        cycle_id=cid,
        structural_cycle_id=structural_cycle_id,
        event_type="v3_structural_group_ready",
        symbol=None,
        price=None,
        meta={"direction": direction},
    )


def _guard_signal_for_single_sided(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    signal_type: str,
) -> Optional[Dict[str, Any]]:
    now = int(time.time())
    row = cur.execute(
        """
        SELECT
            COALESCE(channel_mode, 'two_sided') AS channel_mode,
            COALESCE(known_side, 'both') AS known_side,
            COALESCE(need_rebuild_opposite, 0) AS need_rebuild_opposite,
            opposite_rebuild_deadline_ts
        FROM trading_state
        WHERE id = 1
        """
    ).fetchone()
    if not row:
        return None
    channel_mode = str(row["channel_mode"] or "two_sided")
    known_side = str(row["known_side"] or "both")
    need_rb = int(row["need_rebuild_opposite"] or 0)
    deadline = int(row["opposite_rebuild_deadline_ts"] or 0)
    if channel_mode != "single_sided" or need_rb != 1:
        return None

    blocked = (
        (known_side == "long" and signal_type == "SHORT")
        or (known_side == "short" and signal_type == "LONG")
    )
    if not blocked:
        return None

    if deadline > 0 and now > deadline:
        cur.execute(
            """
            UPDATE trading_state
            SET cycle_phase = 'closed',
                levels_frozen = 0,
                close_reason = 'opposite_rebuild_timeout',
                updated_at = ?
            WHERE id = 1
            """,
            (now,),
        )
        _log_v3_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=structural_cycle_id,
            event_type="v3_opposite_rebuild_timeout",
            symbol=None,
            price=None,
            meta={"signal_type": signal_type, "known_side": known_side, "deadline": deadline},
        )
        return {
            "ok": False,
            "error": "opposite_rebuild_timeout",
            "blocked_signal": signal_type,
            "known_side": known_side,
        }

    _log_v3_event(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=structural_cycle_id,
        event_type="v3_signal_blocked_missing_opposite",
        symbol=None,
        price=None,
        meta={"signal_type": signal_type, "known_side": known_side, "deadline": deadline},
    )
    return {
        "ok": False,
        "error": "opposite_side_not_ready",
        "blocked_signal": signal_type,
        "known_side": known_side,
    }


def _update_state_after_entry(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    direction: str,
    entered: List[str],
    prices: Dict[str, float],
) -> Dict[str, Any]:
    now = int(time.time())
    prev = cur.execute("SELECT COALESCE(position_state, 'none') AS p FROM trading_state WHERE id = 1").fetchone()
    prev_pos = str(prev["p"] if prev else "none")
    is_flip = prev_pos in ("long", "short") and prev_pos != direction
    opposite = "short" if direction == "long" else "long"
    members = _load_cycle_member_symbols(cur, cycle_id=cycle_id)
    scope = entered if not members else [s for s in members if s in entered or s in prices]
    present_opposite = set(_load_cycle_side_symbols(cur, cycle_id=cycle_id, direction=opposite, symbols=scope))
    missing = [s for s in scope if s not in present_opposite]
    attempts_row = cur.execute(
        "SELECT COALESCE(opposite_rebuild_attempts, 0) AS a FROM trading_state WHERE id = 1"
    ).fetchone()
    attempts = int((attempts_row["a"] if attempts_row else 0) or 0)
    rebuild_result: Dict[str, Any] = {
        "attempted": False,
        "inserted": 0,
        "missing": list(missing),
        "structural_opposite_rebuild": None,
    }
    sc_target = (structural_cycle_id or "").strip() or cycle_id
    if st.STRUCTURAL_OPPOSITE_REBUILD_ENABLED and sc_target:
        sr = rebuild_opposite_zone_on_cursor(cur, sc_target, direction)
        rebuild_result["structural_opposite_rebuild"] = {"ok": sr is not None, "cycle_id": sr}
    if missing and st.STRUCTURAL_OPPOSITE_REBUILD_ENABLED:
        rebuild_result["attempted"] = True
        rb = backfill_missing_cycle_side(
            cur,
            cycle_id=cycle_id,
            symbols=missing,
            missing_direction=opposite,
            ref_prices={s: float(prices[s]) for s in missing if s in prices and float(prices[s]) > 0},
        )
        rebuild_result["inserted"] = int(rb.get("inserted", 0))
        rebuild_result["missing"] = list(rb.get("missing", []))
    unresolved_missing = list(rebuild_result.get("missing", missing))
    if unresolved_missing:
        cur.execute(
            """
            UPDATE trading_state
            SET position_state = ?, cycle_phase = 'in_position',
                channel_mode = 'single_sided', known_side = ?,
                need_rebuild_opposite = 1,
                opposite_rebuild_deadline_ts = ?,
                opposite_rebuild_attempts = ?,
                last_rebuild_reason = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                direction,
                direction,
                now + int(st.STRUCTURAL_OPPOSITE_REBUILD_DEADLINE_SEC),
                attempts + (1 if rebuild_result["attempted"] else 0),
                ("post_flip_missing_opposite" if is_flip else "post_entry_missing_opposite"),
                now,
            ),
        )
    else:
        cur.execute(
            """
            UPDATE trading_state
            SET position_state = ?, cycle_phase = 'in_position',
                channel_mode = 'two_sided', known_side = 'both',
                need_rebuild_opposite = 0,
                opposite_rebuild_deadline_ts = NULL,
                opposite_rebuild_attempts = ?,
                last_rebuild_reason = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                direction,
                attempts + (1 if rebuild_result["attempted"] else 0),
                ("post_flip_two_sided" if is_flip else "post_entry_two_sided"),
                now,
            ),
        )
    return rebuild_result


def run_opposite_rebuild_maintenance_tick(
    cur,
    *,
    prices: Dict[str, float],
) -> Dict[str, Any]:
    now = int(time.time())
    row = cur.execute(
        """
        SELECT cycle_id, structural_cycle_id,
               COALESCE(channel_mode, 'two_sided') AS channel_mode,
               COALESCE(known_side, 'both') AS known_side,
               COALESCE(need_rebuild_opposite, 0) AS need_rebuild_opposite,
               opposite_rebuild_deadline_ts,
               COALESCE(opposite_rebuild_attempts, 0) AS opposite_rebuild_attempts
        FROM trading_state
        WHERE id = 1
        """
    ).fetchone()
    if not row or not row["cycle_id"]:
        return {"ok": False, "skipped": "no_cycle"}
    cycle_id = str(row["cycle_id"])
    scid = row["structural_cycle_id"]
    scid = str(scid) if scid else None
    if str(row["channel_mode"] or "two_sided") != "single_sided":
        return {"ok": True, "skipped": "not_single_sided"}
    if int(row["need_rebuild_opposite"] or 0) != 1:
        return {"ok": True, "skipped": "no_pending_rebuild"}
    known_side = str(row["known_side"] or "both")
    if known_side not in ("long", "short"):
        return {"ok": True, "skipped": "known_side_not_single"}
    deadline = int(row["opposite_rebuild_deadline_ts"] or 0)
    attempts = int(row["opposite_rebuild_attempts"] or 0)
    missing_direction = "short" if known_side == "long" else "long"
    if deadline > 0 and now > deadline:
        cur.execute(
            """
            UPDATE trading_state
            SET cycle_phase = 'closed',
                levels_frozen = 0,
                close_reason = 'opposite_rebuild_timeout',
                updated_at = ?
            WHERE id = 1
            """,
            (now,),
        )
        _log_v3_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            event_type="v3_opposite_rebuild_timeout",
            symbol=None,
            price=None,
            meta={"source": "maintenance_tick", "known_side": known_side, "deadline": deadline},
        )
        return {"ok": False, "error": "opposite_rebuild_timeout"}

    members = _load_cycle_member_symbols(cur, cycle_id=cycle_id)
    if not members:
        return {"ok": True, "skipped": "no_cycle_members"}
    side_syms = _load_cycle_side_symbols(cur, cycle_id=cycle_id, direction=known_side, symbols=members)
    if not side_syms:
        return {"ok": True, "skipped": "no_symbols_for_known_side"}
    rb = backfill_missing_cycle_side(
        cur,
        cycle_id=cycle_id,
        symbols=side_syms,
        missing_direction=missing_direction,
        ref_prices={
            s: ref
            for s in side_syms
            for ref in [_ref_price_for_symbol(cur, symbol=s, prices=prices)]
            if ref is not None
        },
        ref_source="maintenance_rebuild",
    )
    unresolved = list(rb.get("missing", []))
    if unresolved:
        cur.execute(
            """
            UPDATE trading_state
            SET opposite_rebuild_attempts = ?,
                last_rebuild_reason = 'maintenance_rebuild_pending',
                updated_at = ?
            WHERE id = 1
            """,
            (attempts + 1, now),
        )
    else:
        cur.execute(
            """
            UPDATE trading_state
            SET channel_mode = 'two_sided',
                known_side = 'both',
                need_rebuild_opposite = 0,
                opposite_rebuild_deadline_ts = NULL,
                opposite_rebuild_attempts = ?,
                last_rebuild_reason = 'maintenance_rebuild_success',
                updated_at = ?
            WHERE id = 1
            """,
            (attempts + 1, now),
        )
    _log_v3_event(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=scid,
        event_type="v3_opposite_rebuild_maintenance",
        symbol=None,
        price=None,
        meta={
            "missing_direction": missing_direction,
            "inserted": int(rb.get("inserted", 0)),
            "missing": unresolved,
            "attempt": attempts + 1,
        },
    )
    if st.OPS_STAGE_SHEETS and not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            export_cycle_levels_sheets_snapshot()
        except Exception:
            logger.exception("cycle levels sheets snapshot export failed after maintenance rebuild")
    return {"ok": True, "inserted": int(rb.get("inserted", 0)), "missing": unresolved}


def process_v3_signal(
    cur,
    *,
    signal_type: str,
    monitor: "LevelCrossMonitor",
    prices: Dict[str, float],
) -> Dict[str, Any]:
    """
    Обработать LONG / SHORT / CANCEL_* после группового сигнала монитора.
    """
    auto_open_results: Optional[List[Dict[str, Any]]] = None
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id FROM trading_state WHERE id = 1"
    ).fetchone()
    if not row or not row["cycle_id"]:
        return {"ok": False, "error": "no_cycle"}
    cycle_id = str(row["cycle_id"])
    scid = row["structural_cycle_id"]
    scid = str(scid) if scid else None

    if signal_type in ("CANCEL_LONG", "CANCEL_SHORT"):
        _log_v3_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            event_type=f"v3_{signal_type.lower()}",
            symbol=None,
            price=None,
            meta={"action": "reset_monitor_flags"},
        )
        cur.execute(
            """
            UPDATE trading_state SET
                allow_long_entry = 1, allow_short_entry = 1,
                updated_at = ?
            WHERE id = 1
            """,
            (int(time.time()),),
        )
        monitor.reset()
        monitor.sync_cycle(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            levels=load_cycle_level_pairs(cur, cycle_id),
        )
        if st.LEVEL_CROSS_TELEGRAM:
            get_telegram_notifier().send_message(
                f"<pre>{escape_html_telegram(signal_type + ' — сценарий сброшен, монитор переинициализирован')}</pre>",
                parse_mode="HTML",
            )
        return {"ok": True, "cancel": signal_type}

    if signal_type in ("LONG", "SHORT"):
        guard = _guard_signal_for_single_sided(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            signal_type=signal_type,
        )
        if guard is not None:
            return guard

    long_pct = float(st.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT)
    short_pct = float(st.ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT)
    entered: List[str] = []
    rejected: List[str] = []

    if signal_type == "LONG":
        confirmation_ids: List[int] = []
        side_levels = _load_cycle_side_levels(cur, cycle_id=cycle_id, direction="long")
        if not side_levels:
            return {"ok": False, "error": "no_long_levels"}
        for symbol, level in side_levels.items():
            current_price = prices.get(symbol)
            if current_price is None:
                rejected.append(symbol)
                continue
            atr = _get_atr(cur, to_bybit_symbol(symbol))
            if atr is None:
                rejected.append(symbol)
                continue
            threshold = long_pct / 100.0 * atr
            criteria_met = float(current_price) >= level - threshold
            logger.debug(
                "EntryGate LONG %s price=%s level=%s atr=%s thr=%s ok=%s",
                symbol,
                current_price,
                level,
                atr,
                threshold,
                criteria_met,
            )
            if criteria_met:
                cid = _confirm(
                    cur,
                    cycle_id=cycle_id,
                    structural_cycle_id=scid,
                    symbol=symbol,
                    direction="long",
                    level_price=level,
                    entry_price=float(current_price),
                    atr=atr,
                    long_pct=long_pct,
                    short_pct=short_pct,
                )
                confirmation_ids.append(cid)
                entered.append(symbol)
            else:
                rejected.append(symbol)

        if entered:
            cur.execute(
                "UPDATE trading_state SET allow_long_entry = 0, updated_at = ? WHERE id = 1",
                (int(time.time()),),
            )
            rebuild_result = _update_state_after_entry(
                cur,
                cycle_id=cycle_id,
                structural_cycle_id=scid,
                direction="long",
                entered=entered,
                prices=prices,
            )
            _log_v3_event(
                cur,
                cycle_id=cycle_id,
                structural_cycle_id=scid,
                event_type="v3_entry_batch_long",
                symbol=None,
                price=None,
                meta={"entered": entered, "rejected": rejected, "opposite_rebuild": rebuild_result},
            )
            if st.LEVEL_CROSS_TELEGRAM:
                will_auto = (
                    st.ENTRY_AUTO_OPEN_AFTER_GATE
                    and st.BYBIT_EXECUTION_ENABLED
                    and bool(confirmation_ids)
                )
                if will_auto:
                    mode = (
                        "лимитные ордера (GTC)"
                        if st.ENTRY_AUTO_OPEN_USE_LIMIT
                        else "рыночный вход и стоп"
                    )
                    gate_msg = (
                        f"Вход LONG по {len(entered)} монетам — выставляем {mode}: "
                        + ",".join(entered)
                    )
                else:
                    gate_msg = "Вход LONG: " + ",".join(entered)
                get_telegram_notifier().send_message(
                    f"<pre>{escape_html_telegram(gate_msg)}</pre>",
                    parse_mode="HTML",
                )
            if (
                st.ENTRY_AUTO_OPEN_AFTER_GATE
                and st.BYBIT_EXECUTION_ENABLED
                and confirmation_ids
            ):
                from trading_bot.data.position_opening import auto_open_after_gate_confirmations

                auto_open_results = auto_open_after_gate_confirmations(
                    cur,
                    confirmation_ids,
                    use_limit=st.ENTRY_AUTO_OPEN_USE_LIMIT,
                )

    elif signal_type == "SHORT":
        confirmation_ids_s: List[int] = []
        side_levels = _load_cycle_side_levels(cur, cycle_id=cycle_id, direction="short")
        if not side_levels:
            return {"ok": False, "error": "no_short_levels"}
        for symbol, level in side_levels.items():
            current_price = prices.get(symbol)
            if current_price is None:
                rejected.append(symbol)
                continue
            atr = _get_atr(cur, to_bybit_symbol(symbol))
            if atr is None:
                rejected.append(symbol)
                continue
            alerted = monitor.get_alerted_status(symbol, "short")
            criteria_met = False
            if not alerted:
                threshold = short_pct / 100.0 * atr
                criteria_met = float(current_price) < level - threshold
            logger.debug(
                "EntryGate SHORT %s price=%s level=%s atr=%s alerted=%s ok=%s",
                symbol,
                current_price,
                level,
                atr,
                alerted,
                criteria_met,
            )
            if criteria_met:
                cid = _confirm(
                    cur,
                    cycle_id=cycle_id,
                    structural_cycle_id=scid,
                    symbol=symbol,
                    direction="short",
                    level_price=level,
                    entry_price=float(current_price),
                    atr=atr,
                    long_pct=long_pct,
                    short_pct=short_pct,
                )
                confirmation_ids_s.append(cid)
                entered.append(symbol)
            else:
                rejected.append(symbol)

        if entered:
            cur.execute(
                "UPDATE trading_state SET allow_short_entry = 0, updated_at = ? WHERE id = 1",
                (int(time.time()),),
            )
            rebuild_result = _update_state_after_entry(
                cur,
                cycle_id=cycle_id,
                structural_cycle_id=scid,
                direction="short",
                entered=entered,
                prices=prices,
            )
            _log_v3_event(
                cur,
                cycle_id=cycle_id,
                structural_cycle_id=scid,
                event_type="v3_entry_batch_short",
                symbol=None,
                price=None,
                meta={"entered": entered, "rejected": rejected, "opposite_rebuild": rebuild_result},
            )
            if st.LEVEL_CROSS_TELEGRAM:
                will_auto = (
                    st.ENTRY_AUTO_OPEN_AFTER_GATE
                    and st.BYBIT_EXECUTION_ENABLED
                    and bool(confirmation_ids_s)
                )
                if will_auto:
                    mode = (
                        "лимитные ордера (GTC)"
                        if st.ENTRY_AUTO_OPEN_USE_LIMIT
                        else "рыночный вход и стоп"
                    )
                    gate_msg = (
                        f"Вход SHORT по {len(entered)} монетам — выставляем {mode}: "
                        + ",".join(entered)
                    )
                else:
                    gate_msg = "Вход SHORT: " + ",".join(entered)
                get_telegram_notifier().send_message(
                    f"<pre>{escape_html_telegram(gate_msg)}</pre>",
                    parse_mode="HTML",
                )
            if (
                st.ENTRY_AUTO_OPEN_AFTER_GATE
                and st.BYBIT_EXECUTION_ENABLED
                and confirmation_ids_s
            ):
                from trading_bot.data.position_opening import auto_open_after_gate_confirmations

                auto_open_results = auto_open_after_gate_confirmations(
                    cur,
                    confirmation_ids_s,
                    use_limit=st.ENTRY_AUTO_OPEN_USE_LIMIT,
                )

    out: Dict[str, Any] = {
        "ok": True,
        "signal": signal_type,
        "entered": entered,
        "rejected": rejected,
    }
    if auto_open_results is not None:
        out["auto_open"] = auto_open_results
    return out


def _confirm(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    symbol: str,
    direction: str,
    level_price: float,
    entry_price: float,
    atr: float,
    long_pct: float,
    short_pct: float,
) -> int:
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO entry_gate_confirmations (
            ts, cycle_id, structural_cycle_id, symbol, direction,
            level_price, entry_price, atr,
            long_atr_threshold_pct, short_atr_threshold_pct, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            ts,
            cycle_id,
            structural_cycle_id,
            symbol,
            direction,
            level_price,
            entry_price,
            atr,
            long_pct,
            short_pct,
        ),
    )
    rid = int(cur.lastrowid)
    _log_v3_event(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=structural_cycle_id,
        event_type=f"v3_entry_confirmed_{direction}",
        symbol=symbol,
        price=entry_price,
        meta={"level": level_price, "atr": atr},
    )
    return rid


__all__ = ["process_v3_signal", "run_opposite_rebuild_maintenance_tick", "signal_structural_ready"]
