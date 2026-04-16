"""
Анализатор тестовых прогонов торгового бота.

Анализирует:
- Базу данных (таблицы: trading_state, position_records, exec_orders, 
  structural_cycles, cycle_levels, ops_stage_runs)
- Лог-файлы (logs/supervisor_*.log)

Выдаёт отчёт на русском языке с подробной статистикой.

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.analyze_test_run [--db PATH] [--logs DIR] [--watch N]
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.config import settings as st
from trading_bot.data.db import get_connection

logger = logging.getLogger(__name__)


class TestAnalyzer:
    """Анализатор тестовых прогонов."""
    
    def __init__(self, db_path: Optional[str] = None, log_dir: Optional[str] = None):
        """
        Args:
            db_path: Путь к базе данных. Если None — использует settings.DB_PATH
            log_dir: Путь к директории с логами. Если None — использует logs/
        """
        self.db_path = db_path or st.DB_PATH
        self.log_dir = log_dir or os.path.join(_REPO, "trading_bot", "logs")
        self.conn = get_connection()
    
    def run(self) -> None:
        """Запустить анализ и вывести отчёт."""
        print("\n" + "=" * 80)
        print("🔍 АНАЛИЗ ТЕСТОВОГО ПРОГОНА")
        print("=" * 80)
        
        self._print_header()
        self._print_cycles()
        self._print_positions()
        self._print_orders()
        self._print_errors()
        self._print_performance_metrics()
        self._print_recommendations()
        
        print("\n" + "=" * 80)
        print("✅ АНАЛИЗ ЗАВЕРШЁН")
        print("=" * 80 + "\n")
    
    def _print_header(self) -> None:
        """Вывести заголовок с общей информацией."""
        print("\n" + "-" * 80)
        print("📋 ОБЩАЯ ИНФОРМАЦИЯ")
        print("-" * 80)
        
        cur = self.conn.cursor()
        
        # Режим тестирования
        row = cur.execute(
            "SELECT last_start_mode FROM trading_state WHERE id = 1"
        ).fetchone()
        
        mode = row["last_start_mode"] if row else None
        is_test = mode == "test_mode"
        
        print(f"Режим: {'🧪 ТЕСТОВЫЙ' if is_test else '🟢 ПРОДАКШЕН'}")
        print(f"Последний режим старта: {mode or 'не установлен'}")
        
        # Время работы
        row = cur.execute(
            "SELECT last_start_ts, last_session_id FROM trading_state WHERE id = 1"
        ).fetchone()
        
        if row and row["last_start_ts"]:
            start_ts = row["last_start_ts"]
            start_dt = datetime.fromtimestamp(start_ts)
            now = datetime.now()
            duration = now - start_dt
            
            print(f"Время начала: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Продолжительность: {self._format_duration(duration.total_seconds())}")
        
        # Символы
        row = cur.execute(
            "SELECT COUNT(DISTINCT symbol) as cnt FROM structural_cycle_symbols"
        ).fetchone()
        print(f"Уникальных символов: {row['cnt'] if row else 0}")
        
        # Циклы
        row = cur.execute(
            "SELECT COUNT(*) as cnt FROM structural_cycles"
        ).fetchone()
        print(f"Всего циклов: {row['cnt'] if row else 0}")
    
    def _print_cycles(self) -> None:
        """Вывести информацию о циклах."""
        print("\n" + "-" * 80)
        print("🔄 СТРУКТУРНЫЕ ЦИКЛЫ")
        print("- * 80)
        
        cur = self.conn.cursor()
        
        # Группировка по фазам
        rows = cur.execute(
            """
            SELECT phase, COUNT(*) as cnt, 
                   GROUP_CONCAT(DISTINCT ref_price_source) as sources
            FROM structural_cycles
            GROUP BY phase
            ORDER BY created_at DESC
            """
        ).fetchall()
        
        if not rows:
            print("  Нет данных о циклах")
            return
        
        for row in rows:
            phase = row["phase"]
            cnt = row["cnt"]
            source = (row["sources"] or "").split(",")[0] or "unknown"
            
            icon = "✅" if phase == "closed" else "🟡" if phase == "armed" else "🔵"
            print(f"  {icon} {phase.upper()}: {cnt} циклов (source={source})")
        
        # Детали последнего цикла
        row = cur.execute(
            """
            SELECT sc.id, sc.phase, sc.created_at, sc.symbols_valid_count,
                   sc.ref_price_source,
                   (SELECT COUNT(*) FROM structural_cycle_symbols s WHERE s.cycle_id = sc.id) as symbols_count
            FROM structural_cycles sc
            ORDER BY sc.created_at DESC
            LIMIT 1
            """
        ).fetchone()
        
        if row:
            print(f"\n  Последний цикл:")
            print(f"    ID: {str(row['id'])[:8] if row['id'] else 'N/A'}")
            print(f"    Фаза: {row['phase']}")
            print(f"    Символы: {row['symbols_count']}")
            print(f"    Источник: {row['ref_price_source']}")
            
            # Уровни
            cycle_id = row["id"]
            if cycle_id:
                levels_row = cur.execute(
                    """
                    SELECT direction, COUNT(*) as cnt
                    FROM cycle_levels
                    WHERE cycle_id = ?
                    GROUP BY direction
                    """,
                    (cycle_id,)
                ).fetchall()
                
                print(f"    Уровни:")
                for lvl in levels_row:
                    print(f"      {lvl['direction'].upper()}: {lvl['cnt']}")
    
    def _print_positions(self) -> None:
        """Вывести информацию о позициях."""
        print("\n" + "-" * 80)
        print("📊 ПОЗИЦИИ")
        print("-" * 80)
        
        cur = self.conn.cursor()
        
        # Общее количество
        row = cur.execute(
            "SELECT COUNT(*) as cnt FROM position_records"
        ).fetchone()
        total = row["cnt"] if row else 0
        print(f"  Всего позиций: {total}")
        
        if total == 0:
            print("  Нет данных о позициях")
            return
        
        # По статусу
        rows = cur.execute(
            """
            SELECT status, COUNT(*) as cnt
            FROM position_records
            GROUP BY status
            ORDER BY cnt DESC
            """
        ).fetchall()
        
        print(f"\n  По статусу:")
        for row in rows:
            status = row["status"]
            cnt = row["cnt"]
            icon = "✅" if status == "closed" else "🔴" if status == "open" else "⏳"
            print(f"    {icon} {status.upper()}: {cnt}")
        
        # По стороне
        rows = cur.execute(
            """
            SELECT side, COUNT(*) as cnt
            FROM position_records
            WHERE status = 'closed'
            GROUP BY side
            """
        ).fetchall()
        
        if rows:
            print(f"\n  Закрытые позиции по стороне:")
            for row in rows:
                side = row["side"]
                cnt = row["cnt"]
                icon = "🟢" if side == "Buy" else "🔴"
                print(f"    {icon} {side.upper()}: {cnt}")
        
        # Причины закрытия
        rows = cur.execute(
            """
            SELECT close_reason, COUNT(*) as cnt
            FROM position_records
            WHERE status = 'closed' AND close_reason IS NOT NULL
            GROUP BY close_reason
            ORDER BY cnt DESC
            """
        ).fetchall()
        
        if rows:
            print(f"\n  Причины закрытия:")
            for row in rows:
                reason = row["close_reason"]
                cnt = row["cnt"]
                print(f"    • {reason}: {cnt}")
        
        # PnL (если есть)
        row = cur.execute(
            """
            SELECT 
                SUM(CASE WHEN close_pnl IS NOT NULL THEN close_pnl ELSE 0 END) as total_pnl,
                COUNT(close_pnl) as pnl_count
            FROM position_records
            WHERE status = 'closed'
            """
        ).fetchone()
        
        if row and row["pnl_count"] and row["pnl_count"] > 0:
            total_pnl = row["total_pnl"] or 0
            avg_pnl = total_pnl / row["pnl_count"]
            
            print(f"\n  PnL:")
            print(f"    Общий: {total_pnl:.2f} USDT")
            print(f"    Средний: {avg_pnl:.2f} USDT")
            print(f"    Сделок с PnL: {row['pnl_count']}")
        
        # Время удержания
        row = cur.execute(
            """
            SELECT 
                AVG((closed_at - created_at) / 60.0) as avg_hold_minutes
            FROM position_records
            WHERE status = 'closed' AND closed_at IS NOT NULL
            """
        ).fetchone()
        
        if row and row["avg_hold_minutes"]:
            print(f"\n  Время удержания:")
            print(f"    Среднее: {self._format_duration(row['avg_hold_minutes'] * 60)}")
    
    def _print_orders(self) -> None:
        """Вывести информацию об ордерах."""
        print("\n" + "-" * 80)
        print("💹 ОРДЕРА")
        print("-" * 80)
        
        cur = self.conn.cursor()
        
        # По статусу
        rows = cur.execute(
            """
            SELECT status, COUNT(*) as cnt
            FROM exec_orders
            GROUP BY status
            ORDER BY cnt DESC
            """
        ).fetchall()
        
        if not rows:
            print("  Нет данных об ордерах")
            return
        
        print(f"  По статусу:")
        for row in rows:
            status = row["status"]
            cnt = row["cnt"]
            icon = "✅" if status == "filled" else "❌" if status in ("cancelled", "canceled") else "⏳"
            print(f"    {icon} {status.upper()}: {cnt}")
        
        # По типу
        rows = cur.execute(
            """
            SELECT side, type, COUNT(*) as cnt
            FROM exec_orders
            WHERE status = 'filled'
            GROUP BY side, type
            ORDER BY cnt DESC
            """
        ).fetchall()
        
        if rows:
            print(f"\n  Исполнённые ордера:")
            for row in rows:
                side = row["side"]
                order_type = row["type"]
                cnt = row["cnt"]
                print(f"    • {side} {order_type}: {cnt}")
    
    def _print_errors(self) -> None:
        """Вывести информацию об ошибках."""
        print("\n" + "-" * 80)
        print("⚠️ ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ")
        print("-" * 80)
        
        cur = self.conn.cursor()
        
        # Ошибки из ops_stage_runs
        rows = cur.execute(
            """
            SELECT stage, status, severity, message, details
            FROM ops_stage_runs
            WHERE status = 'error' OR severity IN ('error', 'critical')
            ORDER BY started_at DESC
            LIMIT 10
            """
        ).fetchall()
        
        if rows:
            print(f"\n  Ошибки из ops_stage_runs ({len(rows)}):")
            for row in rows:
                stage = row["stage"]
                severity = row["severity"] or "error"
                message = (row["message"] or "")[:100]
                icon = "🔴" if severity == "critical" else "🟡"
                print(f"    {icon} [{stage}] {severity}: {message}")
        else:
            print("  Нет ошибок в ops_stage_runs")
        
        # Анализ логов
        self._analyze_log_files()
    
    def _analyze_log_files(self) -> None:
        """Анализировать лог-файлы."""
        print("\n  Анализ логов:")
        
        log_pattern = os.path.join(self.log_dir, "supervisor_*.log")
        log_files = sorted(glob.glob(log_pattern), reverse=True)[:3]
        
        if not log_files:
            print("    Лог-файлы не найдены")
            return
        
        error_count = 0
        warning_count = 0
        
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                for line in lines:
                    if "ERROR" in line or "Exception" in line:
                        error_count += 1
                    elif "WARNING" in line:
                        warning_count += 1
                
                print(f"    Файл: {os.path.basename(log_file)}")
            except Exception as e:
                print(f"    Ошибка чтения {os.path.basename(log_file)}: {e}")
        
        print(f"    Итого: {error_count} ошибок, {warning_count} предупреждений")
    
    def _print_performance_metrics(self) -> None:
        """Вывести метрики производительности."""
        print("\n" + "-" * 80)
        print("⚡ МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ")
        print("-" * 80)
        
        cur = self.conn.cursor()
        
        # Время этапов supervisor
        rows = cur.execute(
            """
            SELECT stage, 
                   AVG((finished_at - started_at)) as avg_duration_sec,
                   COUNT(*) as count
            FROM ops_stage_runs
            WHERE finished_at IS NOT NULL AND started_at IS NOT NULL
            GROUP BY stage
            ORDER BY avg_duration_sec DESC
            """
        ).fetchall()
        
        if rows:
            print(f"\n  Среднее время этапов:")
            for row in rows:
                stage = row["stage"]
                avg_sec = row["avg_duration_sec"] or 0
                count = row["count"] or 0
                print(f"    • {stage}: {avg_sec:.1f} сек (всего {count})")
        
        # Время между закрытием и rebuild противоположной стороны
        row = cur.execute(
            """
            SELECT AVG(
                (SELECT updated_at FROM trading_state WHERE id = 1) 
                - closed_at
            ) as avg_rebuild_time
            FROM position_records
            WHERE status = 'closed' AND closed_at IS NOT NULL
            """
        ).fetchone()
        
        if row and row["avg_rebuild_time"]:
            print(f"\n  Среднее время rebuild противоположной стороны:")
            print(f"    {self._format_duration(row['avg_rebuild_time'])}")
    
    def _print_recommendations(self) -> None:
        """Вывести рекомендации."""
        print("\n" + "-" * 80)
        print("💡 РЕКОМЕНДАЦИИ")
        print("-" * 80)
        
        cur = self.conn.cursor()
        recommendations = []
        
        # Проверка на залипшие позиции
        row = cur.execute(
            """
            SELECT COUNT(*) as cnt
            FROM position_records
            WHERE status = 'open'
            """
        ).fetchone()
        
        if row and row["cnt"] and row["cnt"] > 0:
            recommendations.append(f"🔴 Есть {row['cnt']} открытых позиций — проверьте, не застряли ли они")
        
        # Проверка на ошибки
        row = cur.execute(
            """
            SELECT COUNT(*) as cnt
            FROM ops_stage_runs
            WHERE status = 'error' OR severity = 'critical'
            """
        ).fetchone()
        
        if row and row["cnt"] and row["cnt"] > 5:
            recommendations.append(f"🟡 Много ошибок ({row['cnt']}) — проверьте логи")
        
        # Проверка на успешные сделки
        row = cur.execute(
            """
            SELECT COUNT(*) as cnt
            FROM position_records
            WHERE status = 'closed' AND close_pnl > 0
            """
        ).fetchone()
        
        total_closed = cur.execute(
            "SELECT COUNT(*) FROM position_records WHERE status = 'closed'"
        ).fetchone()["cnt"]
        
        if total_closed and total_closed > 0 and row["cnt"]:
            win_rate = (row["cnt"] / total_closed) * 100
            if win_rate < 40:
                recommendations.append(f"🟡 Низкая winning rate ({win_rate:.1f}%) — проверьте уровни")
            elif win_rate > 70:
                recommendations.append(f"🟢 Хорошая winning rate ({win_rate:.1f}%)")
        
        # Проверка на тестовый режим
        row = cur.execute(
            "SELECT last_start_mode FROM trading_state WHERE id = 1"
        ).fetchone()
        
        if row and row["last_start_mode"] == "test_mode":
            recommendations.append("🧪 Работает в ТЕСТОВОМ режиме — уровни искусственные")
        
        if recommendations:
            for rec in recommendations:
                print(f"  {rec}")
        else:
            print("  ✅ Нет критических рекомендаций")
    
    def _format_duration(self, seconds: float) -> str:
        """Форматировать длительность."""
        if seconds < 60:
            return f"{seconds:.1f} сек"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} мин"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} час"
    
    def close(self) -> None:
        """Закрыть соединение с БД."""
        self.conn.close()


def watch_mode(interval_sec: int = 10) -> None:
    """Режим наблюдения — обновлять отчёт каждые N секунд."""
    print(f"👁️  Режим наблюдения (обновление каждые {interval_sec} сек)")
    print("Нажмите Ctrl+C для выхода\n")
    
    analyzer = TestAnalyzer()
    
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            analyzer.run()
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n\n⏹️  Наблюдение остановлено")
    finally:
        analyzer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Анализатор тестовых прогонов")
    parser.add_argument("--db", type=str, help="Путь к базе данных")
    parser.add_argument("--logs", type=str, help="Путь к директории с логами")
    parser.add_argument("--watch", type=int, help="Режим наблюдения (интервал в сек)")
    
    args = parser.parse_args()
    
    if args.watch:
        watch_mode(args.watch)
        return
    
    analyzer = TestAnalyzer(
        db_path=args.db,
        log_dir=args.logs
    )
    
    try:
        analyzer.run()
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
