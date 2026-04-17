"""
Монитор пересечений уровней LONG/SHORT по логике tutorial_v3/traiding_monitor.py.

Источник уровней: замороженные строки `cycle_levels` + `trading_state` (без изменений structural).
Групповой порог: число уникальных монет с алертом за окно (len(long_alerts) == MIN_ALERTS_COUNT
для старта окна), затем ожидание ALERT_TIMEOUT_MINUTES; отмена при избытке символов в окне.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
from typing import Any, Dict, List, Optional, Tuple

from trading_bot.config import settings as st
from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier

logger = logging.getLogger(__name__)


def load_cycle_level_pairs(cur, cycle_id: str) -> Dict[str, Dict[str, float]]:
    rows = cur.execute(
        """
        SELECT symbol, direction, level_price
        FROM cycle_levels
        WHERE cycle_id = ? AND is_active = 1 AND level_step = 1
        """,
        (cycle_id,),
    ).fetchall()
    out: Dict[str, Dict[str, float]] = {}
    for r in rows:
        sym = str(r["symbol"])
        d = str(r["direction"])
        out.setdefault(sym, {})[d] = float(r["level_price"])
    # Монета может участвовать только в long или только в short наборе.
    return out


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


@dataclass
class LevelCrossMonitor:
    """Состояние цикла мониторинга (как TradingMonitor; in-memory)."""

    levels: Dict[str, Dict[str, float]] = field(default_factory=dict)
    cycle_id: str = ""
    structural_cycle_id: Optional[str] = None
    prev_prices: Dict[str, Optional[float]] = field(default_factory=dict)
    alerted: Dict[str, Dict[str, bool]] = field(default_factory=dict)
    alerts_history: List[Dict[str, Any]] = field(default_factory=list)
    last_alert_count: int = 0
    long_alerts: Dict[str, List[datetime]] = field(default_factory=dict)
    short_alerts: Dict[str, List[datetime]] = field(default_factory=dict)
    long_window_start: Optional[datetime] = None
    short_window_start: Optional[datetime] = None

    def reset(self) -> None:
        self.cycle_id = ""
        self.structural_cycle_id = None
        self.levels.clear()
        self.prev_prices.clear()
        self.alerted.clear()
        self.alerts_history.clear()
        self.last_alert_count = 0
        self.long_alerts.clear()
        self.short_alerts.clear()
        self.long_window_start = None
        self.short_window_start = None

    def sync_cycle(
        self,
        cur,
        *,
        cycle_id: str,
        structural_cycle_id: Optional[str],
        levels: Dict[str, Dict[str, float]],
    ) -> None:
        if cycle_id != self.cycle_id:
            self.reset()
            self.cycle_id = cycle_id
            self.structural_cycle_id = structural_cycle_id
            self.levels = dict(levels)
            for sym in self.levels:
                self.prev_prices[sym] = None
                self.alerted[sym] = {"long": False, "short": False}
            logger.info("LevelCrossMonitor: new cycle_id=%s symbols=%s", cycle_id, len(self.levels))
        else:
            self.levels = dict(levels)
            self.structural_cycle_id = structural_cycle_id
            for sym in self.levels:
                if sym not in self.prev_prices:
                    self.prev_prices[sym] = None
                self.alerted.setdefault(sym, {"long": False, "short": False})

    def get_alerted_status(self, symbol: str, signal_type: str) -> bool:
        return self.alerted.get(symbol, {}).get(signal_type.lower(), False)

    def check_levels(
        self,
        cur,
        *,
        symbol: str,
        current_price: float,
        allow_long: bool,
        allow_short: bool,
    ) -> None:
        levels = self.levels.get(symbol, {})
        long_level = levels.get("long")
        short_level = levels.get("short")

        prev = self.prev_prices.get(symbol)
        if prev is None:
            # Первый тик: проверяем, является ли текущая цена уже пересечением
            # (например, если cycle начался, когда цена уже была рядом с уровнем)
            self.prev_prices[symbol] = current_price
            logger.info(
                "LevelCross: %s init price=%.4f long_level=%s short_level=%s",
                symbol, current_price,
                f"{long_level:.4f}" if long_level else "N/A",
                f"{short_level:.4f}" if short_level else "N/A"
            )
            return

        prev = float(prev)
        timestamp = _utc_naive()

        if allow_long and long_level is not None and not self.alerted[symbol]["long"]:
            if prev > long_level and current_price <= long_level:
                msg = f"Пересечение уровня LONG для {symbol}: цена {current_price} <= {long_level}"
                logger.info("%s", msg)
                self.alerts_history.append(
                    {
                        "symbol": symbol,
                        "type": "LONG",
                        "price": current_price,
                        "level": long_level,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                _log_v3_event(
                    cur,
                    cycle_id=self.cycle_id,
                    structural_cycle_id=self.structural_cycle_id,
                    event_type="v3_cross_long",
                    symbol=symbol,
                    price=current_price,
                    meta={"level": long_level, "prev": prev},
                )
                self.alerted[symbol]["long"] = True
                if st.LEVEL_CROSS_TELEGRAM and st.LEVEL_CROSS_TELEGRAM_CROSSINGS:
                    get_telegram_notifier().send_message(
                        f"<pre>{escape_html_telegram(msg)}</pre>",
                        parse_mode="HTML",
                    )
        elif (
            not allow_long
            and long_level is not None
            and prev > long_level
            and current_price <= long_level
        ):
            # ДОБАВЛЕНО ЛОГИРОВАНИЕ для отладки
            logger.info("LONG cross DETECTED but blocked: %s prev=%.4f level=%.4f current=%.4f allow_long=%s",
                        symbol, prev, long_level, current_price, allow_long)
            if st.LEVEL_CROSS_TELEGRAM and st.LEVEL_CROSS_TELEGRAM_CROSSINGS:
                msg = f"⚠️ Пересечение LONG заблокировано: {symbol} цена {current_price} <= {long_level}"
                get_telegram_notifier().send_message(
                    f"<pre>{escape_html_telegram(msg)}</pre>",
                    parse_mode="HTML",
                )

        if allow_short and short_level is not None and not self.alerted[symbol]["short"]:
            if prev < short_level and current_price >= short_level:
                msg = f"Пересечение уровня SHORT для {symbol}: цена {current_price} >= {short_level}"
                logger.info("%s", msg)
                self.alerts_history.append(
                    {
                        "symbol": symbol,
                        "type": "SHORT",
                        "price": current_price,
                        "level": short_level,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                _log_v3_event(
                    cur,
                    cycle_id=self.cycle_id,
                    structural_cycle_id=self.structural_cycle_id,
                    event_type="v3_cross_short",
                    symbol=symbol,
                    price=current_price,
                    meta={"level": short_level, "prev": prev},
                )
                self.alerted[symbol]["short"] = True
                if st.LEVEL_CROSS_TELEGRAM and st.LEVEL_CROSS_TELEGRAM_CROSSINGS:
                    get_telegram_notifier().send_message(
                        f"<pre>{escape_html_telegram(msg)}</pre>",
                        parse_mode="HTML",
                    )
        elif (
            not allow_short
            and short_level is not None
            and prev < short_level
            and current_price >= short_level
        ):
            # ДОБАВЛЕНО ЛОГИРОВАНИЕ для отладки
            logger.info("SHORT cross DETECTED but blocked: %s prev=%.4f level=%.4f current=%.4f allow_short=%s",
                        symbol, prev, short_level, current_price, allow_short)
            if st.LEVEL_CROSS_TELEGRAM and st.LEVEL_CROSS_TELEGRAM_CROSSINGS:
                msg = f"⚠️ Пересечение SHORT заблокировано: {symbol} цена {current_price} >= {short_level}"
                get_telegram_notifier().send_message(
                    f"<pre>{escape_html_telegram(msg)}</pre>",
                    parse_mode="HTML",
                )

        self.prev_prices[symbol] = current_price

    def process_alerts(self) -> None:
        current_alert_count = len(self.alerts_history)
        if current_alert_count <= self.last_alert_count:
            return
        new_alerts = self.alerts_history[self.last_alert_count : current_alert_count]
        min_n = int(st.LEVEL_CROSS_MIN_ALERTS_COUNT)
        for alert in new_alerts:
            symbol = alert["symbol"]
            alert_type = alert["type"]
            timestamp = datetime.strptime(alert["timestamp"], "%Y-%m-%d %H:%M:%S")
            if alert_type == "LONG":
                self.long_alerts.setdefault(symbol, []).append(timestamp)
                if len(self.long_alerts) == min_n and self.long_window_start is None:
                    self.long_window_start = timestamp
            elif alert_type == "SHORT":
                self.short_alerts.setdefault(symbol, []).append(timestamp)
                if len(self.short_alerts) == min_n and self.short_window_start is None:
                    self.short_window_start = timestamp
        self.last_alert_count = current_alert_count

    def check_entry_conditions(self, cur) -> List[str]:
        out: List[str] = []
        current_time = _utc_naive()
        min_n = int(st.LEVEL_CROSS_MIN_ALERTS_COUNT)
        timeout_min = float(st.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES)

        # Проверяем текущее состояние позиции из БД
        pos_row = cur.execute("""
            SELECT position_state FROM trading_state WHERE id = 1
        """).fetchone()
        current_pos_state = str(pos_row['position_state']) if pos_row else 'none'

        if self.long_window_start:
            long_count = len(self.long_alerts)
            time_diff = (current_time - self.long_window_start).total_seconds() / 60.0
            if long_count >= min_n and time_diff >= timeout_min:
                # БЛОКИРОВКА: если уже есть LONG позиция
                if current_pos_state == 'long':
                    logger.info("V3: LONG сигнал ИГНОРИРОВАН - уже есть LONG позиция")
                    # Очищаем окно, чтобы не повторять проверку каждый тик
                    self.long_alerts.clear()
                    self.long_window_start = None
                else:
                    logger.info("V3: entry signal LONG (group)")
                    if st.LEVEL_CROSS_TELEGRAM:
                        # Добавляем список символов в уведомление
                        symbols_list = ', '.join(list(self.long_alerts.keys())[:5])
                        if len(self.long_alerts) > 5:
                            symbols_list += f" и ещё {len(self.long_alerts)-5}"
                        msg = f"🟢 СИГНАЛ LONG: {len(self.long_alerts)} монет пересекли уровни\n{symbols_list}"
                        get_telegram_notifier().send_message(
                            f"<pre>{escape_html_telegram(msg)}</pre>",
                            parse_mode="HTML",
                        )
                    _log_v3_event(
                        cur,
                        cycle_id=self.cycle_id,
                        structural_cycle_id=self.structural_cycle_id,
                        event_type="v3_entry_signal_long",
                        symbol=None,
                        price=None,
                        meta={"symbols": list(self.long_alerts.keys()), "count": long_count},
                    )
                    out.append("LONG")
                    self.long_alerts.clear()
                    self.long_window_start = None

        if self.short_window_start:
            short_count = len(self.short_alerts)
            time_diff = (current_time - self.short_window_start).total_seconds() / 60.0
            if short_count >= min_n and time_diff >= timeout_min:
                # БЛОКИРОВКА: если уже есть SHORT позиция
                if current_pos_state == 'short':
                    logger.info("V3: SHORT сигнал ИГНОРИРОВАН - уже есть SHORT позиция")
                    # Очищаем окно, чтобы не повторять проверку каждый тик
                    self.short_alerts.clear()
                    self.short_window_start = None
                else:
                    logger.info("V3: entry signal SHORT (group)")
                    if st.LEVEL_CROSS_TELEGRAM:
                        # Добавляем список символов в уведомление
                        symbols_list = ', '.join(list(self.short_alerts.keys())[:5])
                        if len(self.short_alerts) > 5:
                            symbols_list += f" и ещё {len(self.short_alerts)-5}"
                        msg = f"🔴 СИГНАЛ SHORT: {len(self.short_alerts)} монет пересекли уровни\n{symbols_list}"
                        get_telegram_notifier().send_message(
                            f"<pre>{escape_html_telegram(msg)}</pre>",
                            parse_mode="HTML",
                        )
                    _log_v3_event(
                        cur,
                        cycle_id=self.cycle_id,
                        structural_cycle_id=self.structural_cycle_id,
                        event_type="v3_entry_signal_short",
                        symbol=None,
                        price=None,
                        meta={"symbols": list(self.short_alerts.keys()), "count": short_count},
                    )
                    out.append("SHORT")
                    self.short_alerts.clear()
                    self.short_window_start = None

        return out

    def check_cancellation_conditions(self, cur) -> List[str]:
        out: List[str] = []
        current_time = _utc_naive()
        min_n = int(st.LEVEL_CROSS_MIN_ALERTS_COUNT)
        max_add = int(st.LEVEL_CROSS_MAX_ADDITIONAL_ALERTS)
        timeout_min = float(st.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES)

        if self.long_window_start:
            time_diff = (current_time - self.long_window_start).total_seconds() / 60.0
            long_count = len(self.long_alerts)
            if time_diff <= timeout_min and long_count >= min_n:
                other_long_count = long_count - min_n
                if other_long_count >= max_add:
                    logger.info("V3: cancel LONG scenario (too many symbols in window)")
                    if st.LEVEL_CROSS_TELEGRAM:
                        get_telegram_notifier().send_message(
                            f"<pre>{escape_html_telegram('Отмена сценария LONG')}</pre>",
                            parse_mode="HTML",
                        )
                    _log_v3_event(
                        cur,
                        cycle_id=self.cycle_id,
                        structural_cycle_id=self.structural_cycle_id,
                        event_type="v3_cancel_long",
                        symbol=None,
                        price=None,
                        meta={"long_count": long_count},
                    )
                    out.append("CANCEL_LONG")
                    self.long_alerts.clear()
                    self.long_window_start = None

        if self.short_window_start:
            time_diff = (current_time - self.short_window_start).total_seconds() / 60.0
            short_count = len(self.short_alerts)
            if time_diff <= timeout_min and short_count >= min_n:
                other_short_count = short_count - min_n
                if other_short_count >= max_add:
                    logger.info("V3: cancel SHORT scenario (too many symbols in window)")
                    if st.LEVEL_CROSS_TELEGRAM:
                        get_telegram_notifier().send_message(
                            f"<pre>{escape_html_telegram('Отмена сценария SHORT')}</pre>",
                            parse_mode="HTML",
                        )
                    _log_v3_event(
                        cur,
                        cycle_id=self.cycle_id,
                        structural_cycle_id=self.structural_cycle_id,
                        event_type="v3_cancel_short",
                        symbol=None,
                        price=None,
                        meta={"short_count": short_count},
                    )
                    out.append("CANCEL_SHORT")
                    self.short_alerts.clear()
                    self.short_window_start = None

        return out


_GLOBAL_MONITOR: Optional[LevelCrossMonitor] = None


def get_level_cross_monitor() -> LevelCrossMonitor:
    global _GLOBAL_MONITOR
    if _GLOBAL_MONITOR is None:
        _GLOBAL_MONITOR = LevelCrossMonitor()
    return _GLOBAL_MONITOR


def run_level_cross_tick(
    cur,
    *,
    prices: Dict[str, float],
    monitor: Optional[LevelCrossMonitor] = None,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Один тик: обновить пересечения, вернуть сигналы для EntryGate.
    `cur` — открытый cursor в транзакции; вызывающий делает commit.
    """
    mon = monitor or get_level_cross_monitor()
    row = cur.execute(
        """
        SELECT cycle_id, structural_cycle_id, levels_frozen,
               COALESCE(allow_long_entry, 1) AS allow_long_entry,
               COALESCE(allow_short_entry, 1) AS allow_short_entry
        FROM trading_state WHERE id = 1
        """
    ).fetchone()
    summary: Dict[str, Any] = {"skipped": None, "signals": []}
    if not row or not row["cycle_id"]:
        summary["skipped"] = "no_cycle_id"
        return [], summary
    if not int(row["levels_frozen"] or 0):
        summary["skipped"] = "levels_not_frozen"
        return [], summary

    cycle_id = str(row["cycle_id"])
    scid = row["structural_cycle_id"]
    scid = str(scid) if scid else None
    allow_long = bool(int(row["allow_long_entry"] or 0))
    allow_short = bool(int(row["allow_short_entry"] or 0))
    levels = load_cycle_level_pairs(cur, cycle_id)
    if not levels:
        summary["skipped"] = "no_cycle_levels"
        return [], summary

    mon.sync_cycle(cur, cycle_id=cycle_id, structural_cycle_id=scid, levels=levels)

    for sym in mon.levels:
        px = prices.get(sym)
        if px is None or float(px) <= 0:
            continue
        mon.check_levels(cur, symbol=sym, current_price=float(px), allow_long=allow_long, allow_short=allow_short)

    mon.process_alerts()
    signals: List[str] = []
    signals.extend(mon.check_entry_conditions(cur))
    signals.extend(mon.check_cancellation_conditions(cur))
    summary["signals"] = list(signals)
    summary["symbols"] = len(mon.levels)
    summary["long_count"] = sum(1 for lv in mon.levels.values() if "long" in lv)
    summary["short_count"] = sum(1 for lv in mon.levels.values() if "short" in lv)

    if st.LEVEL_CROSS_TICK_SUMMARY_LOG:
        n_lv = len(mon.levels)
        priced = sum(
            1 for s in mon.levels if prices.get(s) is not None and float(prices[s]) > 0
        )
        min_n = int(st.LEVEL_CROSS_MIN_ALERTS_COUNT)
        timeout_min = float(st.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES)
        now_t = _utc_naive()
        long_n = len(mon.long_alerts)
        short_n = len(mon.short_alerts)
        hist_n = len(mon.alerts_history)
        long_age: Optional[float] = None
        short_age: Optional[float] = None
        if mon.long_window_start:
            long_age = (now_t - mon.long_window_start).total_seconds() / 60.0
        if mon.short_window_start:
            short_age = (now_t - mon.short_window_start).total_seconds() / 60.0
        long_pending = ""
        if mon.long_window_start and long_n >= min_n and long_age is not None and long_age < timeout_min:
            long_pending = f"LONG waiting timeout ({long_age:.1f}/{timeout_min} min, {long_n}>={min_n} syms)"
        short_pending = ""
        if mon.short_window_start and short_n >= min_n and short_age is not None and short_age < timeout_min:
            short_pending = f"SHORT waiting timeout ({short_age:.1f}/{timeout_min} min, {short_n}>={min_n} syms)"
        if mon.long_window_start and long_n < min_n:
            long_pending = f"LONG need more symbols ({long_n}/{min_n})"
        if mon.short_window_start and short_n < min_n:
            short_pending = f"SHORT need more symbols ({short_n}/{min_n})"
        extra = " ".join(x for x in (long_pending, short_pending) if x).strip()
        logger.info(
            "LevelCrossTick cycle_id=%s symbols=%s priced=%s/%s allow_long=%s allow_short=%s "
            "crosses_hist=%s long_alerts_sym=%s short_alerts_sym=%s "
            "min_n=%s timeout_min=%s signals=%s%s",
            cycle_id,
            n_lv,
            priced,
            n_lv,
            int(allow_long),
            int(allow_short),
            hist_n,
            long_n,
            short_n,
            min_n,
            timeout_min,
            signals,
            f" | {extra}" if extra else "",
        )

    return signals, summary


__all__ = [
    "LevelCrossMonitor",
    "get_level_cross_monitor",
    "load_cycle_level_pairs",
    "run_level_cross_tick",
]
