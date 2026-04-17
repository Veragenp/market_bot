"""
Анализатор тестовых прогонов торгового бота v2.

Генерирует подробные отчёты в папке logs/:
- report_summary_YYYYMMDD_HHMMSS.txt - общий отчёт по всем циклам
- report_CYCLEID_YYYYMMDD_HHMMSS.txt - детальный отчёт по конкретному циклу

Запуск:
  python -m trading_bot.scripts.analyze_test_run_v2
  python -m trading_bot.scripts.analyze_test_run_v2 --cycle-id 17d52b26-3be2-4ab2-8758-8443d2be6a55
  python -m trading_bot.scripts.analyze_test_run_v2 --monitor 300
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Optional

# UTF-8 для Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.config import settings as st
from trading_bot.data.db import get_connection


def analyze_summary(output_file: str) -> None:
    """Генерация общего отчёта."""
    conn = get_connection()
    cur = conn.cursor()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        def write(text=""):
            f.write(text + "\n")
        
        write("=" * 80)
        write(f"ОБЩИЙ ОТЧЁТ ТОРГОВОГО КОНТУРА")
        write(f"Генерация: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        write("=" * 80)
        
        # Общая информация
        write("\n" + "-" * 80)
        write("ОБЩАЯ ИНФОРМАЦИЯ")
        write("-" * 80)
        
        row = cur.execute("SELECT last_start_mode, cycle_phase, levels_frozen, cycle_id FROM trading_state WHERE id = 1").fetchone()
        if row:
            write(f"Режим: {'ТЕСТОВЫЙ' if row['last_start_mode'] == 'test_mode' else 'ПРОДАКШЕН'}")
            write(f"Фаза: {row['cycle_phase'] or 'N/A'}")
            write(f"Уровни заморожены: {'Да' if row['levels_frozen'] else 'Нет'}")
            write(f"Активный цикл: {str(row['cycle_id'])[:8] if row['cycle_id'] else 'Нет'}")
        
        # Циклы
        write("\n" + "-" * 80)
        write("СТРУКТУРНЫЕ ЦИКЛЫ (ПОСЛЕДНИЕ 10)")
        write("-" * 80)
        
        rows = cur.execute("SELECT id, phase, created_at FROM structural_cycles ORDER BY created_at DESC LIMIT 10").fetchall()
        for row in rows:
            ts = datetime.fromtimestamp(row['created_at']) if row['created_at'] else None
            time_str = ts.strftime("%H:%M:%S") if ts else "?"
            cycle_id = str(row['id'])[:8] if row['id'] else "-"
            write(f"  [{cycle_id}] {row['phase']:15} @ {time_str}")
        
        # Позиции
        write("\n" + "-" * 80)
        write("ПОЗИЦИИ (ВСЕ)")
        write("-" * 80)
        
        rows = cur.execute("""
            SELECT symbol, side, status, qty, entry_price, exit_price, realized_pnl, close_reason
            FROM position_records ORDER BY created_at DESC LIMIT 20
        """).fetchall()
        
        if rows:
            write(f"  {'Symbol':12} {'Side':6} {'Status':10} {'Qty':6} {'Entry':10} {'PnL':10} {'Reason'}")
            write("  " + "-" * 70)
            for pos in rows:
                pnl = f"{pos['realized_pnl']:.2f}" if pos['realized_pnl'] else "-"
                reason = (pos['close_reason'] or "-")[:15]
                write(f"  {pos['symbol']:12} {pos['side']:6} {pos['status']:10} {pos['qty'] or '?':6} {pos['entry_price'] or '?':10.4f} {pnl:10} {reason}")
        else:
            write("  Нет позиций")
        
        # Ордера
        write("\n" + "-" * 80)
        write("ОРДЕРА (ВСЕ)")
        write("-" * 80)
        
        rows = cur.execute("""
            SELECT symbol, side, order_type, status, avg_fill_price, qty, filled_qty
            FROM exec_orders ORDER BY created_at DESC LIMIT 20
        """).fetchall()
        
        if rows:
            write(f"  {'Symbol':12} {'Side':6} {'Type':8} {'Status':10} {'Price':10} {'Qty'}")
            write("  " + "-" * 60)
            for o in rows:
                write(f"  {o['symbol']:12} {o['side']:6} {o['order_type']:8} {o['status']:10} {o['avg_fill_price'] or '?':10.4f} {o['filled_qty'] or o['qty'] or '?'}")
        else:
            write("  Нет ордеров")
        
        # Анализ ошибок
        write("\n" + "-" * 80)
        write("АНАЛИЗ ОШИБОК ИЗ ЛОГОВ")
        write("-" * 80)
        
        log_dir = os.path.join(_REPO, "trading_bot", "logs")
        log_files = sorted(glob.glob(os.path.join(log_dir, "supervisor_*.log")), reverse=True)[:5]
        
        error_types = defaultdict(int)
        total_errors = 0
        
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as lf:
                    for line in lf:
                        if "ERROR" in line:
                            total_errors += 1
                            if "database is locked" in line.lower():
                                error_types["database_locked"] += 1
                            elif "UNIQUE constraint" in line:
                                error_types["unique_constraint"] += 1
                            elif "tvDatafeed" in line and "signin" in line:
                                error_types["tradingview_signin"] += 1
                            elif "WebSocket" in line and "404" in line:
                                error_types["websocket_404"] += 1
                            else:
                                error_types["other"] += 1
            except:
                pass
        
        write(f"\n  Всего ошибок (последние 5 логов): {total_errors}")
        if error_types:
            write("\n  Типы ошибок:")
            for et, cnt in sorted(error_types.items(), key=lambda x: -x[1]):
                write(f"    {et}: {cnt}")
        
        write("\n" + "=" * 80)
        write("КОНЕЦ ОТЧЁТА")
        write("=" * 80)
    
    conn.close()


def _fmt_ts(ts: Optional[int]) -> str:
    """Форматировать timestamp в читаемую дату."""
    if ts is None:
        return "N/A"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(seconds: float) -> str:
    """Форматировать длительность в секундах."""
    if seconds < 60:
        return f"{seconds:.1f} сек"
    elif seconds < 3600:
        return f"{seconds/60:.1f} мин"
    else:
        return f"{seconds/3600:.1f} час"


def _write_cycle_timeline(f, cur, cycle_id: str) -> None:
    """Раздел 1: Хронология цикла из structural_events."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("1. ХРОНОЛОГИЯ ЦИКЛА")
    write("-" * 80)
    
    # События из structural_events
    events = cur.execute("""
        SELECT event_type, ts, price, symbol, meta_json
        FROM structural_events
        WHERE cycle_id = ?
        ORDER BY ts
    """, (cycle_id,)).fetchall()
    
    if events:
        write(f"  {'Time':19} {'Event':20} {'Symbol':12} {'Price':12} {'Details'}")
        write("  " + "-" * 80)
        for evt in events:
            ts_str = datetime.fromtimestamp(evt['ts']).strftime("%H:%M:%S") if evt['ts'] else "?"
            event_type = evt['event_type']
            symbol = (evt['symbol'] or '-')[:12]
            price = f"{evt['price']:.4f}" if evt['price'] else "-"
            
            # Парсим meta для краткого описания
            meta = evt['meta_json']
            details = "-"
            if meta:
                import json
                try:
                    m = json.loads(meta)
                    if 'to' in m:
                        details = f"→ {m['to']}"
                    elif 'cycle_levels_rows' in m:
                        details = f"{m['cycle_levels_rows']} уровней"
                    elif 'long_count' in m:
                        details = f"L={m.get('long_count',0)} S={m.get('short_count',0)}"
                except:
                    pass
            
            write(f"  {ts_str:19} {event_type:20} {symbol:12} {price:12} {details}")
    else:
        write("  Нет данных о событиях цикла")
    
    # Этапы supervisor
    write("\n  Этапы supervisor:")
    stages = cur.execute("""
        SELECT stage, status, started_at, finished_at, duration_ms, message
        FROM ops_stage_runs
        WHERE cycle_id = ?
        ORDER BY started_at
    """, (cycle_id,)).fetchall()
    
    if stages:
        for s in stages:
            duration = f"{s['duration_ms']/1000:.1f}с" if s['duration_ms'] else "?"
            status_icon = "✅" if s['status'] == 'ok' else "❌" if s['status'] == 'failed' else "🔵"
            write(f"    {status_icon} [{s['stage']}] {s['status']:8} {duration:10} - {s['message'] or '-'}")
    else:
        write("    Нет данных об этапах")


def _write_cycle_levels(f, cur, cycle_id: str) -> None:
    """Раздел 2: Уровни цикла с подробной информацией."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("2. УРОВНИ ЦИКЛА")
    write("-" * 80)
    
    # LONG уровни
    long_levels = cur.execute("""
        SELECT symbol, level_price, tier, volume_peak, distance_atr, ref_price, is_active
        FROM cycle_levels
        WHERE cycle_id = ? AND direction = 'long'
        ORDER BY symbol
    """, (cycle_id,)).fetchall()
    
    write("\n  LONG уровни:")
    if long_levels:
        write(f"    {'Symbol':12} {'Price':12} {'Tier':15} {'Vol Peak':10} {'Dist(ATR)':10} {'Active'}")
        write("    " + "-" * 70)
        for lvl in long_levels:
            active = "✅" if lvl['is_active'] else "❌"
            tier = (lvl['tier'] or '-')[:15]
            vol = f"{lvl['volume_peak']:.2f}" if lvl['volume_peak'] else "-"
            dist = f"{lvl['distance_atr']:.2f}" if lvl['distance_atr'] else "-"
            write(f"    {lvl['symbol']:12} {lvl['level_price']:12.4f} {tier:15} {vol:10} {dist:10} {active}")
        write(f"    Всего: {len(long_levels)} уровней")
    else:
        write("    Нет LONG уровней")
    
    # SHORT уровни
    short_levels = cur.execute("""
        SELECT symbol, level_price, tier, volume_peak, distance_atr, ref_price, is_active
        FROM cycle_levels
        WHERE cycle_id = ? AND direction = 'short'
        ORDER BY symbol
    """, (cycle_id,)).fetchall()
    
    write("\n  SHORT уровни:")
    if short_levels:
        write(f"    {'Symbol':12} {'Price':12} {'Tier':15} {'Vol Peak':10} {'Dist(ATR)':10} {'Active'}")
        write("    " + "-" * 70)
        for lvl in short_levels:
            active = "✅" if lvl['is_active'] else "❌"
            tier = (lvl['tier'] or '-')[:15]
            vol = f"{lvl['volume_peak']:.2f}" if lvl['volume_peak'] else "-"
            dist = f"{lvl['distance_atr']:.2f}" if lvl['distance_atr'] else "-"
            write(f"    {lvl['symbol']:12} {lvl['level_price']:12.4f} {tier:15} {vol:10} {dist:10} {active}")
        write(f"    Всего: {len(short_levels)} уровней")
    else:
        write("    Нет SHORT уровней")


def _write_cycle_comparison(f, cur, cycle_id: str) -> None:
    """Раздел 3: Сравнение с предыдущим циклом."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("3. СРАВНЕНИЕ С ПРЕДЫДУЩИМ ЦИКЛОМ")
    write("-" * 80)
    
    # Находим предыдущий цикл
    prev_cycle = cur.execute("""
        SELECT id, created_at
        FROM structural_cycles
        WHERE created_at < (SELECT created_at FROM structural_cycles WHERE id = ?)
        ORDER BY created_at DESC
        LIMIT 1
    """, (cycle_id,)).fetchone()
    
    if not prev_cycle:
        write("  Предыдущий цикл не найден (первый цикл)")
        return
    
    prev_id = prev_cycle['id']
    write(f"  Предыдущий цикл: {prev_id[:8]} @ {_fmt_ts(prev_cycle['created_at'])}")
    
    # Уровни текущего цикла
    curr_levels = cur.execute("""
        SELECT symbol, direction, level_price
        FROM cycle_levels WHERE cycle_id = ?
    """, (cycle_id,)).fetchall()
    
    # Уровни предыдущего цикла
    prev_levels = cur.execute("""
        SELECT symbol, direction, level_price
        FROM cycle_levels WHERE cycle_id = ?
    """, (prev_id,)).fetchall()
    
    # Создаём словари для сравнения
    curr_dict = {(r['symbol'], r['direction']): r['level_price'] for r in curr_levels}
    prev_dict = {(r['symbol'], r['direction']): r['level_price'] for r in prev_levels}
    
    curr_keys = set(curr_dict.keys())
    prev_keys = set(prev_dict.keys())
    
    new_keys = curr_keys - prev_keys
    removed_keys = prev_keys - curr_keys
    common_keys = curr_keys & prev_keys
    
    changed = [(k, prev_dict[k], curr_dict[k]) for k in common_keys if prev_dict[k] != curr_dict[k]]
    unchanged = [(k, curr_dict[k]) for k in common_keys if prev_dict[k] == curr_dict[k]]
    
    write(f"\n  Новые уровни ({len(new_keys)}):")
    if new_keys:
        for sym, dir_ in sorted(new_keys):
            write(f"    ➕ {sym:12} {dir_.upper():6} {curr_dict[(sym, dir_)]:.4f}")
    else:
        write("    Нет новых уровней")
    
    write(f"\n  Исчезнувшие уровни ({len(removed_keys)}):")
    if removed_keys:
        for sym, dir_ in sorted(removed_keys):
            write(f"    ➖ {sym:12} {dir_.upper():6} {prev_dict[(sym, dir_)]:.4f}")
    else:
        write("    Нет исчезнувших уровней")
    
    write(f"\n  Изменившиеся уровни ({len(changed)}):")
    if changed:
        for (sym, dir_), old_px, new_px in sorted(changed):
            diff = new_px - old_px
            diff_pct = (diff / old_px * 100) if old_px else 0
            arrow = "↑" if diff > 0 else "↓"
            write(f"    🔄 {sym:12} {dir_.upper():6} {old_px:.4f} {arrow} {new_px:.4f} ({diff_pct:+.2f}%)")
    else:
        write("    Нет изменений")
    
    write(f"\n  Без изменений ({len(unchanged)}):")
    if len(unchanged) <= 10:
        for (sym, dir_), px in unchanged:
            write(f"    • {sym:12} {dir_.upper():6} {px:.4f}")
    else:
        write(f"    {len(unchanged)} уровней без изменений (показано 10)")
        for (sym, dir_), px in unchanged[:10]:
            write(f"    • {sym:12} {dir_.upper():6} {px:.4f}")


def _write_level_crossings(f, cur, cycle_id: str) -> None:
    """Раздел 4: События пересечений уровней."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("4. СОБЫТИЯ ПЕРЕСЕЧЕНИЙ УРОВНЕЙ")
    write("-" * 80)
    
    events = cur.execute("""
        SELECT symbol, event_type, price, ts, distance_to_long_atr, distance_to_short_atr, atr_used, meta_json
        FROM entry_detector_events
        WHERE cycle_id = ?
        ORDER BY ts
    """, (cycle_id,)).fetchall()
    
    if not events:
        write("  Нет данных о пересечениях")
        return
    
    write(f"  {'Time':19} {'Symbol':12} {'Event':20} {'Price':12} {'Details'}")
    write("  " + "-" * 80)
    
    for evt in events:
        ts_str = datetime.fromtimestamp(evt['ts']).strftime("%H:%M:%S") if evt['ts'] else "?"
        symbol = evt['symbol']
        event_type = evt['event_type']
        price = f"{evt['price']:.4f}" if evt['price'] else "-"
        
        details = []
        if evt['distance_to_long_atr'] is not None:
            details.append(f"L_dist={evt['distance_to_long_atr']:.2f}ATR")
        if evt['distance_to_short_atr'] is not None:
            details.append(f"S_dist={evt['distance_to_short_atr']:.2f}ATR")
        if evt['atr_used'] is not None:
            details.append(f"ATR={evt['atr_used']:.4f}")
        
        write(f"  {ts_str:19} {symbol:12} {event_type:20} {price:12} {' | '.join(details)}")
    
    write(f"\n  Итого событий: {len(events)}")


def _write_entry_gates(f, cur, cycle_id: str) -> None:
    """Раздел 5: Проверки ATR-порогов при входе."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("5. ПРОВЕРКИ ВХОДА (ENTRY GATE)")
    write("-" * 80)
    
    confs = cur.execute("""
        SELECT symbol, direction, level_price, entry_price, atr, long_atr_threshold_pct, short_atr_threshold_pct, meta_json
        FROM entry_gate_confirmations
        WHERE cycle_id = ?
        ORDER BY ts
    """, (cycle_id,)).fetchall()
    
    if not confs:
        write("  Нет данных о проверках входа")
        return
    
    write(f"  {'Symbol':12} {'Dir':6} {'Level':12} {'Entry':12} {'ATR':10} {'Threshold':12} {'Distance'}")
    write("  " + "-" * 85)
    
    passed = 0
    failed = 0
    
    for c in confs:
        sym = c['symbol']
        dir_ = c['direction'].upper()
        level = f"{c['level_price']:.4f}"
        entry = f"{c['entry_price']:.4f}"
        atr = f"{c['atr']:.4f}" if c['atr'] else "-"
        
        threshold_pct = c['long_atr_threshold_pct'] if dir_ == 'LONG' else c['short_atr_threshold_pct']
        threshold = f"{threshold_pct:.1f}%" if threshold_pct else "-"
        
        # Расстояние в ATR
        if c['entry_price'] and c['level_price'] and c['atr']:
            dist_atr = abs(c['entry_price'] - c['level_price']) / c['atr']
            dist_pct = abs(c['entry_price'] - c['level_price']) / c['level_price'] * 100
            dist_str = f"{dist_atr:.2f}ATR ({dist_pct:.2f}%)"
            
            # Прошла ли проверка
            threshold_atr = threshold_pct / 100.0 if threshold_pct else 0
            if dist_atr <= threshold_atr:
                status = "✅ PASS"
                passed += 1
            else:
                status = "❌ FAIL"
                failed += 1
        else:
            dist_str = "-"
            status = "?"
        
        write(f"  {sym:12} {dir_:6} {level:12} {entry:12} {atr:10} {threshold:12} {dist_str:20} {status}")
    
    write(f"\n  Итого: {passed} прошли, {failed} не прошли")


def _write_flip_analysis(f, cur, cycle_id: str) -> None:
    """Раздел 6: Анализ переворотов рынка (FLIP)."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("6. FLIP СОБЫТИЯ (ПЕРЕВОРОТ РЫНКА)")
    write("-" * 80)
    
    # Ищем закрытия позиций с причиной flip
    flip_positions = cur.execute("""
        SELECT symbol, side, status, entry_price, exit_price, realized_pnl, 
               close_reason, created_at, closed_at
        FROM position_records
        WHERE cycle_id = ? AND close_reason LIKE '%flip%' OR close_reason LIKE '%FLIP%'
    """, (cycle_id,)).fetchall()
    
    if flip_positions:
        write(f"  {'Symbol':12} {'Side':6} {'Entry':12} {'Exit':12} {'PnL':10} {'Closed At'}")
        write("  " + "-" * 70)
        for pos in flip_positions:
            exit_px = f"{pos['exit_price']:.4f}" if pos['exit_price'] else "-"
            pnl = f"{pos['realized_pnl']:.2f}" if pos['realized_pnl'] else "-"
            closed = datetime.fromtimestamp(pos['closed_at']).strftime("%H:%M:%S") if pos['closed_at'] else "-"
            write(f"  {pos['symbol']:12} {pos['side'].upper():6} {pos['entry_price'] or '?':12.4f} {exit_px:12} {pnl:10} {closed}")
    else:
        write("  Нет FLIP событий")
    
    # Информация о состоянии для rebuild
    state = cur.execute("""
        SELECT need_rebuild_opposite, opposite_rebuild_attempts, last_rebuild_reason,
               opposite_rebuild_deadline_ts
        FROM trading_state
        WHERE structural_cycle_id = ?
    """, (cycle_id,)).fetchone()
    
    if state:
        write("\n  Rebuild информация:")
        write(f"    need_rebuild_opposite: {'Да' if state['need_rebuild_opposite'] else 'Нет'}")
        write(f"    opposite_rebuild_attempts: {state['opposite_rebuild_attempts']}")
        write(f"    last_rebuild_reason: {state['last_rebuild_reason'] or '-'}")


def _write_positions(f, cur, cycle_id: str) -> None:
    """Раздел 7: Позиции с детальной информацией."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("7. ПОЗИЦИИ")
    write("-" * 80)
    
    positions = cur.execute("""
        SELECT symbol, side, status, qty, entry_price, entry_price_fact, 
               exit_price, exit_price_fact, realized_pnl, fees, close_reason,
               created_at, opened_at, closed_at
        FROM position_records
        WHERE cycle_id = ?
        ORDER BY created_at
    """, (cycle_id,)).fetchall()
    
    if not positions:
        write("  Нет позиций")
        return
    
    write(f"  {'Symbol':12} {'Side':6} {'Status':10} {'Qty':8} {'Entry':12} {'Exit':12} {'PnL':10} {'Reason'}")
    write("  " + "-" * 85)
    
    total_pnl = 0
    for pos in positions:
        entry = f"{pos['entry_price_fact'] or pos['entry_price'] or '?'}"
        entry = f"{float(entry):.4f}" if entry != '?' and entry else "?"
        
        exit_px = pos['exit_price_fact'] or pos['exit_price']
        exit_str = f"{float(exit_px):.4f}" if exit_px else "-"
        
        pnl = f"{pos['realized_pnl']:.2f}" if pos['realized_pnl'] else "-"
        if pos['realized_pnl']:
            total_pnl += pos['realized_pnl']
        
        reason = (pos['close_reason'] or '-')[:15]
        
        status_icon = "🟡" if pos['status'] == 'pending' else "🟢" if pos['status'] == 'open' else "🔴" if pos['status'] == 'closed' else "⚪"
        
        write(f"  {status_icon} {pos['symbol']:11} {pos['side'].upper():6} {pos['status']:10} {pos['qty'] or '?':8} {entry:12} {exit_str:12} {pnl:10} {reason}")
    
    write(f"\n  Итого позиций: {len(positions)}")
    write(f"  Общий PnL: {total_pnl:.2f}")


def _write_orders(f, cur, cycle_id: str) -> None:
    """Раздел 8: Ордера."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("8. ОРДЕРА")
    write("-" * 80)
    
    orders = cur.execute("""
        SELECT symbol, side, order_type, status, price, avg_fill_price, qty, filled_qty, order_role
        FROM exec_orders
        WHERE cycle_id = ?
        ORDER BY created_at
    """, (cycle_id,)).fetchall()
    
    if not orders:
        write("  Нет ордеров")
        return
    
    write(f"  {'Symbol':12} {'Side':6} {'Type':8} {'Role':8} {'Status':10} {'Price':12} {'Qty':8} {'Filled'}")
    write("  " + "-" * 85)
    
    for o in orders:
        price = f"{o['price']:.4f}" if o['price'] else "-"
        fill_px = f"{o['avg_fill_price']:.4f}" if o['avg_fill_price'] else "-"
        filled = f"{o['filled_qty'] or '?'}" if o['filled_qty'] else o['qty'] or '?'
        role = o['order_role'] or '-'
        
        status_icon = "✅" if o['status'] == 'filled' else "🟡" if o['status'] == 'partially_filled' else "🔵" if o['status'] == 'open' else "❌" if o['status'] == 'cancelled' else "⚪"
        
        write(f"  {status_icon} {o['symbol']:11} {o['side'].upper():6} {o['order_type']:8} {role:8} {o['status']:10} {price:12} {o['qty']:8} {filled}")


def _write_stage_performance(f, cur, cycle_id: str) -> None:
    """Раздел 9: Производительность этапов supervisor."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("9. ПРОИЗВОДИТЕЛЬНОСТЬ ЭТАПОВ SUPERVISOR")
    write("-" * 80)
    
    stages = cur.execute("""
        SELECT stage, status, started_at, finished_at, duration_ms, message
        FROM ops_stage_runs
        WHERE cycle_id = ? AND started_at IS NOT NULL AND finished_at IS NOT NULL
        ORDER BY started_at
    """, (cycle_id,)).fetchall()
    
    if not stages:
        write("  Нет данных о времени этапов")
        return
    
    write(f"  {'Stage':25} {'Status':10} {'Duration':12} {'Message'}")
    write("  " + "-" * 80)
    
    total_duration = 0
    for s in stages:
        duration_ms = s['duration_ms'] or 0
        total_duration += duration_ms
        duration_str = f"{duration_ms/1000:.2f}с"
        status_icon = "✅" if s['status'] == 'ok' else "❌" if s['status'] == 'failed' else "🔵"
        
        write(f"  {status_icon} {s['stage']:24} {s['status']:10} {duration_str:12} {s['message'] or '-'}")
    
    write(f"\n  Общее время этапов: {_fmt_duration(total_duration/1000)}")


def _write_errors(f, cur, cycle_id: str) -> None:
    """Раздел 10: Ошибки и предупреждения."""
    def write(text=""):
        f.write(text + "\n")
    
    write("\n" + "-" * 80)
    write("10. ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ")
    write("-" * 80)
    
    errors = cur.execute("""
        SELECT stage, status, severity, message, started_at
        FROM ops_stage_runs
        WHERE cycle_id = ? AND (status = 'failed' OR severity IN ('error', 'critical'))
        ORDER BY started_at
    """, (cycle_id,)).fetchall()
    
    if errors:
        write(f"  {'Time':19} {'Stage':20} {'Severity':10} {'Message'}")
        write("  " + "-" * 80)
        for e in errors:
            ts_str = datetime.fromtimestamp(e['started_at']).strftime("%H:%M:%S") if e['started_at'] else "?"
            severity = e['severity'] or 'error'
            icon = "🔴" if severity == 'critical' else "🟡"
            write(f"  {icon} {ts_str:19} {e['stage']:20} {severity:10} {e['message'] or '-'}")
    else:
        write("  Нет ошибок")


def analyze_cycle(cycle_id: str, output_file: str) -> None:
    """Генерация детального отчёта по циклу."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Если ID короткий (меньше 36 символов), ищем по частичному совпадению
    full_cycle_id = cycle_id
    if len(cycle_id) < 36:
        # Сначала проверяем structural_cycles
        row = cur.execute("""
            SELECT id FROM structural_cycles WHERE id LIKE ? || '%'
            ORDER BY created_at DESC LIMIT 1
        """, (cycle_id,)).fetchone()
        if row:
            full_cycle_id = row['id']
        else:
            # Попробуем по cycle_levels
            row = cur.execute("""
                SELECT DISTINCT cycle_id FROM cycle_levels WHERE cycle_id LIKE ? || '%'
                ORDER BY cycle_id DESC LIMIT 1
            """, (cycle_id,)).fetchone()
            if row:
                full_cycle_id = row['cycle_id']
            else:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"❌ Цикл не найден: {cycle_id}\n")
                conn.close()
                return
        
    with open(output_file, 'w', encoding='utf-8') as f:
        def write(text=""):
            f.write(text + "\n")
        
        # Заголовок
        write("=" * 80)
        write(f"ДЕТАЛЬНЫЙ ОТЧЁТ ЦИКЛА: {full_cycle_id[:8]}")
        write(f"Генерация: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        write("=" * 80)
    
        # Информация о цикле
        row = cur.execute("""
            SELECT id, phase, created_at, symbols_valid_count, params_json
            FROM structural_cycles WHERE id = ?
        """, (full_cycle_id,)).fetchone()
        
        if not row:
            write(f"❌ Цикл не найден: {cycle_id}")
            conn.close()
            return
        
        write("\n" + "-" * 80)
        write("ИНФОРМАЦИЯ О ЦИКЛЕ")
        write("-" * 80)
        write(f"ID: {row['id']}")
        write(f"Фаза: {row['phase']}")
        write(f"Создан: {_fmt_ts(row['created_at'])}")
        write(f"Символы: {row['symbols_valid_count']}")
        
        # 1. Хронология
        _write_cycle_timeline(f, cur, full_cycle_id)
        
        # 2. Уровни
        _write_cycle_levels(f, cur, full_cycle_id)
        
        # 3. Сравнение с предыдущим
        _write_cycle_comparison(f, cur, full_cycle_id)
        
        # 4. Пересечения
        _write_level_crossings(f, cur, full_cycle_id)
        
        # 5. Проверки входа
        _write_entry_gates(f, cur, full_cycle_id)
        
        # 6. Flip события
        _write_flip_analysis(f, cur, full_cycle_id)
        
        # 7. Позиции
        _write_positions(f, cur, full_cycle_id)
        
        # 8. Ордера
        _write_orders(f, cur, full_cycle_id)
        
        # 9. Производительность этапов
        _write_stage_performance(f, cur, full_cycle_id)
        
        # 10. Ошибки
        _write_errors(f, cur, full_cycle_id)
        
        # Конец
        write("\n" + "=" * 80)
        write("КОНЕЦ ОТЧЁТА ЦИКЛА")
        write("=" * 80)
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Анализатор тестовых прогонов v2")
    parser.add_argument("--cycle-id", type=str, help="ID цикла для детального отчёта")
    parser.add_argument("--output-dir", type=str, help="Папка для отчётов (по умолчанию: logs/)")
    parser.add_argument("--monitor", type=int, nargs="?", const=300, help="Авто-мониторинг (интервал в сек)")
    
    args = parser.parse_args()
    
    output_dir = args.output_dir or os.path.join(_REPO, "trading_bot", "logs")
    os.makedirs(output_dir, exist_ok=True)
    
    if args.monitor:
        print(f"Авто-мониторинг каждые {args.monitor} сек в {output_dir}")
        iteration = 0
        while True:
            iteration += 1
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"report_summary_{timestamp}.txt")
            analyze_summary(output_file)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Отчёт {iteration}: {os.path.basename(output_file)}")
            import time
            time.sleep(args.monitor)
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if args.cycle_id:
        output_file = os.path.join(output_dir, f"report_{args.cycle_id[:8]}_{timestamp}.txt")
        print(f"Генерация отчёта для цикла {args.cycle_id[:8]}...")
        analyze_cycle(args.cycle_id, output_file)
    else:
        output_file = os.path.join(output_dir, f"report_summary_{timestamp}.txt")
        print(f"Генерация общего отчёта...")
        analyze_summary(output_file)
    
    print(f"✅ Отчёт сохранён: {output_file}")


if __name__ == "__main__":
    main()
