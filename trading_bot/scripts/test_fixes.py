"""
Тестирование исправлений:
1. database locked в export_cycle_levels_sheets_snapshot
2. Telegram уведомления
3. build_structural_trading_levels_df
"""

from __future__ import annotations

import sys
import os

# Добавляем путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'entrypoints'))

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def test_export_cycle_levels_sheets_snapshot():
    """Тестируем что export не падает с database locked."""
    print("=" * 80)
    print("ТЕСТ: export_cycle_levels_sheets_snapshot")
    print("=" * 80)
    
    try:
        from trading_bot.data.cycle_levels_db import export_cycle_levels_sheets_snapshot
        result = export_cycle_levels_sheets_snapshot()
        print(f"✅ УСПЕХ: {result}")
        return True
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_build_structural_trading_levels_df():
    """Тестируем что функция работает с PricePoint."""
    print("\n" + "=" * 80)
    print("ТЕСТ: build_structural_trading_levels_df")
    print("=" * 80)
    
    try:
        from trading_bot.data.structural_ops_notify import build_structural_trading_levels_df
        from trading_bot.data.db import get_connection
        
        conn = get_connection()
        cur = conn.cursor()
        
        # Получаем последний cycle_id
        row = cur.execute(
            "SELECT id FROM structural_cycles ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        
        if not row:
            print("⚠️  Нет structural_cycles в БД")
            conn.close()
            return False
        
        cycle_id = row["id"]
        print(f"Cycle ID: {cycle_id[:8]}")
        
        df = build_structural_trading_levels_df(cycle_id)
        print(f"✅ УСПЕХ: DataFrame с {len(df)} строками")
        print(f"Columns: {list(df.columns)}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_telegram_config():
    """Проверяем что Telegram настроен."""
    print("\n" + "=" * 80)
    print("ТЕСТ: Telegram конфигурация")
    print("=" * 80)
    
    from trading_bot.config import settings as st
    
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")
    
    print(f"TELEGRAM_BOT_TOKEN: {'***' if token else 'NOT SET'}")
    print(f"TELEGRAM_CHAT_ID: {'***' if chat_id else 'NOT SET'}")
    print(f"ENTRY_DETECTOR_TELEGRAM_START: {st.ENTRY_DETECTOR_TELEGRAM_START}")
    print(f"LEVEL_CROSS_TELEGRAM: {st.LEVEL_CROSS_TELEGRAM}")
    print(f"STRUCTURAL_OPS_TELEGRAM: {st.STRUCTURAL_OPS_TELEGRAM}")
    
    if token and chat_id:
        print("✅ Telegram настроен")
        return True
    else:
        print("❌ Telegram НЕ настроен (отсутствуют token/chat_id)")
        return False


def test_database_access():
    """Тестируем что БД не заблокирована."""
    print("\n" + "=" * 80)
    print("ТЕСТ: Базовый доступ к БД")
    print("=" * 80)
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Простой запрос
        row = cur.execute("SELECT COUNT(*) as c FROM structural_cycles").fetchone()
        count = row["c"] if row else 0
        print(f"structural_cycles rows: {count}")
        
        row = cur.execute("SELECT COUNT(*) as c FROM cycle_levels").fetchone()
        count = row["c"] if row else 0
        print(f"cycle_levels rows: {count}")
        
        conn.close()
        print("✅ БД доступна")
        return True
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False


def main():
    print("\n" + "=" * 80)
    print("ТЕСТИРОВАНИЕ ИСПРАВЛЕНИЙ")
    print("=" * 80 + "\n")
    
    init_db()
    run_migrations()
    
    results = []
    
    results.append(("Database access", test_database_access()))
    results.append(("Telegram config", test_telegram_config()))
    results.append(("Build structural levels DF", test_build_structural_trading_levels_df()))
    results.append(("Export cycle levels sheets", test_export_cycle_levels_sheets_snapshot()))
    
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
