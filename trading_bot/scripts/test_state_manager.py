"""
Тестирование State Manager и режимов старта.
"""

from __future__ import annotations

import sys
import os
import codecs
import locale

# Устанавливаем UTF-8 кодировку для Windows
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'entrypoints'))


def test_determine_start_mode():
    """Тестируем определение режима старта."""
    print("=" * 80)
    print("ТЕСТ: determine_start_mode")
    print("=" * 80)
    
    from trading_bot.data.state_manager import determine_start_mode
    
    mode, session_id, details = determine_start_mode()
    
    print(f"\n✅ Результат:")
    print(f"  Mode: {mode}")
    print(f"  Session ID: {session_id[:8]}")
    print(f"  Details: {details}")
    
    # Ожидаем FRESH_START (так как cycle_id = NULL после миграции)
    if mode == "FRESH_START":
        print("\n✅ PASS: Ожидаемый режим FRESH_START")
        return True
    else:
        print(f"\n⚠️  Ожидали FRESH_START, получили {mode}")
        return False


def test_migration_v23():
    """Проверяем что миграция v23 применена."""
    print("\n" + "=" * 80)
    print("ТЕСТ: Migration v23")
    print("=" * 80)
    
    from trading_bot.data.db import get_connection
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Проверка версии БД
        row = cur.execute("SELECT MAX(version) FROM db_version").fetchone()
        version = row[0] if row else 0
        print(f"\nТекущая версия БД: {version}")
        
        if version < 23:
            print("❌ FAIL: Миграция v23 не применена")
            return False
        
        print("✅ Миграция v23 применена")
        
        # Проверка полей в position_records
        cur.execute("PRAGMA table_info(position_records)")
        pos_cols = {row[1] for row in cur.fetchall()}
        
        required_cols = [
            'exchange_position_id',
            'last_sync_ts',
            'sync_status',
            'last_sync_error'
        ]
        
        missing_pos = [c for c in required_cols if c not in pos_cols]
        if missing_pos:
            print(f"❌ FAIL: Отсутствуют поля в position_records: {missing_pos}")
            return False
        
        print(f"✅ Поля position_records: {', '.join(required_cols)}")
        
        # Проверка полей в trading_state
        cur.execute("PRAGMA table_info(trading_state)")
        ts_cols = {row[1] for row in cur.fetchall()}
        
        required_ts = [
            'last_session_id',
            'last_start_ts',
            'last_start_mode',
            'opposite_rebuild_in_progress'
        ]
        
        missing_ts = [c for c in required_ts if c not in ts_cols]
        if missing_ts:
            print(f"❌ FAIL: Отсутствуют поля в trading_state: {missing_ts}")
            return False
        
        print(f"✅ Поля trading_state: {', '.join(required_ts)}")
        
        # Проверка таблицы runtime_state
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_state'")
        row = cur.fetchone()
        if not row:
            print("❌ FAIL: Таблица runtime_state не создана")
            return False
        
        print("✅ Таблица runtime_state создана")
        
        return True
        
    finally:
        conn.close()


def test_get_trading_state():
    """Тестируем get_trading_state()."""
    print("\n" + "=" * 80)
    print("ТЕСТ: get_trading_state")
    print("=" * 80)
    
    from trading_bot.data.state_manager import get_trading_state
    
    state = get_trading_state()
    
    if not state:
        print("❌ FAIL: trading_state пуст")
        return False
    
    print(f"\n✅ trading_state:")
    for key, value in state.items():
        if key in ['cycle_id', 'structural_cycle_id', 'last_session_id']:
            value_str = f"{value[:8]}..." if value else "NULL"
        else:
            value_str = str(value)
        print(f"  {key:30s}: {value_str}")
    
    return True


def test_handle_fresh_start():
    """Тестируем handle_fresh_start()."""
    print("\n" + "=" * 80)
    print("ТЕСТ: handle_fresh_start")
    print("=" * 80)
    
    from trading_bot.data.state_manager import handle_fresh_start, get_trading_state
    
    result = handle_fresh_start()
    
    if not result.get("ok"):
        print(f"❌ FAIL: {result.get('error')}")
        return False
    
    print(f"\n✅ Результат:")
    print(f"  Mode: {result.get('mode')}")
    print(f"  Session ID: {result.get('session_id', '')[:8]}")
    
    # Проверка что trading_state обновился
    state = get_trading_state()
    if state.get('cycle_phase') != 'arming':
        print(f"❌ FAIL: cycle_phase != 'arming'")
        return False
    
    if state.get('last_start_mode') != 'fresh':
        print(f"❌ FAIL: last_start_mode != 'fresh'")
        return False
    
    print(f"✅ trading_state обновлён: phase=arming, mode=fresh")
    
    return True


def main():
    print("\n" + "=" * 80)
    print("ТЕСТИРОВАНИЕ STATE MANAGER")
    print("=" * 80 + "\n")
    
    results = []
    
    results.append(("Migration v23", test_migration_v23()))
    results.append(("get_trading_state", test_get_trading_state()))
    results.append(("determine_start_mode", test_determine_start_mode()))
    results.append(("handle_fresh_start", test_handle_fresh_start()))
    
    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(r for _, r in results)
    print("\n" + ("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ" if all_passed else "❌ ЕСТЬ ОШИБКИ"))
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
