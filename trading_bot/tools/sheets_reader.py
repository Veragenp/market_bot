"""
Чтение Google Sheets (gspread service account), тот же способ авторизации, что у SheetsExporter.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

DEFAULT_HEADER_ROW = (
    "stable_level_id",
    "price",
    "tier",
    "is_active",
)


class SheetsReader:
    """Клиент для чтения и лёгкого создания листов (без очистки существующих данных)."""

    def __init__(
        self,
        credentials_path: str,
        *,
        spreadsheet_title: str | None = None,
        spreadsheet_url: str | None = None,
        spreadsheet_id: str | None = None,
    ) -> None:
        try:
            import gspread  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "gspread is required. Install with `pip install gspread`."
            ) from exc

        self._gspread = gspread
        self._client = gspread.service_account(filename=credentials_path)
        if spreadsheet_url:
            self._spreadsheet = self._client.open_by_url(spreadsheet_url)
        elif spreadsheet_id:
            self._spreadsheet = self._client.open_by_key(spreadsheet_id)
        elif spreadsheet_title:
            self._spreadsheet = self._client.open(spreadsheet_title)
        else:
            raise ValueError(
                "One of spreadsheet_title, spreadsheet_url or spreadsheet_id is required."
            )

    @property
    def spreadsheet_title(self) -> str:
        return str(self._spreadsheet.title)

    def list_worksheet_titles(self) -> list[str]:
        return [ws.title for ws in self._spreadsheet.worksheets()]

    def get_or_create_worksheet(
        self,
        title: str,
        *,
        rows: int = 2000,
        cols: int = 12,
        header_row: Sequence[str] | None = None,
        assume_missing: bool = False,
    ) -> Any:
        """
        Возвращает лист по имени; если нет — создаёт и опционально пишет первую строку-шапку.
        """
        gspread = self._gspread
        if assume_missing:
            ws = self._spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
            created = True
        else:
            try:
                ws = self._spreadsheet.worksheet(title)
                created = False
            except gspread.exceptions.WorksheetNotFound:
                ws = self._spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
                created = True
        if created and header_row:
            ws.update(
                range_name="A1",
                values=[list(header_row)],
            )
        elif created:
            ws.update(
                range_name="A1",
                values=[list(DEFAULT_HEADER_ROW)],
            )
        return ws

    def read_worksheet_dicts(
        self,
        title: str,
        *,
        expected_headers: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Читает лист: первая строка — ключи (нормализуются в lower snake-friendly).
        Пустые строки (без stable_level_id) отфильтровывает вызывающий код.
        """
        ws = self._spreadsheet.worksheet(title)
        values = ws.get_all_values()
        if not values:
            return []
        raw_headers = [str(c).strip() for c in values[0]]
        headers = [_normalize_header_key(h) for h in raw_headers]
        if expected_headers:
            exp = [_normalize_header_key(h) for h in expected_headers]
            if headers[: len(exp)] != exp and not _headers_contain_all(headers, exp):
                # мягко: если шапка не совпадает, всё равно парсим по позициям имён
                pass
        rows: list[dict[str, Any]] = []
        for row in values[1:]:
            d: dict[str, Any] = {}
            for i, key in enumerate(headers):
                if not key:
                    continue
                d[key] = row[i].strip() if i < len(row) else ""
            rows.append(d)
        return rows


def _normalize_header_key(h: str) -> str:
    s = str(h).strip().lower().replace(" ", "_")
    return s


def _headers_contain_all(headers: list[str], required: Sequence[str]) -> bool:
    hs = set(headers)
    return all(r in hs for r in required)


def worksheet_title_for_symbol(symbol: str) -> str:
    """Имя вкладки: BTC/USDT → BTC_USDT (без слэшей и пробелов)."""
    return symbol.replace("/", "_").replace(" ", "_").replace("\\", "_")


__all__ = [
    "DEFAULT_HEADER_ROW",
    "SheetsReader",
    "worksheet_title_for_symbol",
]
