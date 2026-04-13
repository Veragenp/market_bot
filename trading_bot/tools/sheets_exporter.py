from __future__ import annotations

from typing import Any, Sequence

import pandas as pd


class SheetsExporter:
    """Google Sheets exporter backed by gspread service account."""

    def __init__(
        self,
        credentials_path: str,
        spreadsheet_title: str | None = None,
        spreadsheet_url: str | None = None,
        spreadsheet_id: str | None = None,
    ) -> None:
        try:
            import gspread  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "gspread is required for Sheets export. Install with `pip install gspread`."
            ) from exc

        self._client = gspread.service_account(filename=credentials_path)
        if spreadsheet_url:
            self._spreadsheet = self._client.open_by_url(spreadsheet_url)
        elif spreadsheet_id:
            self._spreadsheet = self._client.open_by_key(spreadsheet_id)
        elif spreadsheet_title:
            self._spreadsheet = self._client.open(spreadsheet_title)
        else:
            raise ValueError("One of spreadsheet_title, spreadsheet_url or spreadsheet_id is required.")

    def _get_or_create_worksheet(self, worksheet_name: str, rows: int = 2000, cols: int = 20):
        try:
            return self._spreadsheet.worksheet(worksheet_name)
        except Exception:
            return self._spreadsheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)

    def export_dataframe_to_sheet(
        self,
        df: pd.DataFrame,
        sheet_title: str,
        worksheet_name: str,
    ) -> None:
        """
        Write DataFrame into worksheet, replacing existing contents.
        sheet_title is kept for compatibility and validated against initialized sheet.
        """
        if sheet_title and sheet_title != self._spreadsheet.title:
            # Keep non-fatal mismatch to support open_by_url/open_by_key workflows.
            sheet_title = self._spreadsheet.title

        ws = self._get_or_create_worksheet(worksheet_name)
        ws.clear()

        values: Sequence[Sequence[object]]
        if df.empty:
            values = [list(df.columns)]
        else:
            values = [list(df.columns)] + df.where(pd.notnull(df), "").values.tolist()
        ws.update(values=values, range_name="A1")

    def append_row(self, worksheet_name: str, values: Sequence[Any]) -> None:
        """Добавить одну строку в конец листа (для журналов событий)."""
        ws = self._get_or_create_worksheet(worksheet_name)
        ws.append_row([str(v) if v is not None else "" for v in values], value_input_option="USER_ENTERED")
