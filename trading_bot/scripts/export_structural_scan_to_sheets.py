"""
Запуск structural-scan (пара L/U снизу/сверху по силе в top-K, MAD по пулу) и выгрузка в Google Sheets.

  PYTHONPATH=. python -m trading_bot.scripts.export_structural_scan_to_sheets
  PYTHONPATH=. python -m trading_bot.scripts.export_structural_scan_to_sheets --no-freeze

Лист: trading_bot.config.settings.STRUCTURAL_LEVELS_REPORT_WORKSHEET
Книга: MARKET_AUDIT_SHEET_* / export_to_sheets.py (как vp_local).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.config.settings import STRUCTURAL_LEVELS_REPORT_WORKSHEET
from trading_bot.data.db import get_connection
from trading_bot.data.structural_cycle_db import run_structural_pipeline
from trading_bot.tools.sheets_exporter import SheetsExporter

_EP = os.path.join(_REPO, "trading_bot", "entrypoints")
if _EP not in sys.path:
    sys.path.insert(0, _EP)
import export_to_sheets as es  # noqa: E402


def _load_params_json(cur, cycle_id: str) -> dict:
    row = cur.execute("SELECT params_json FROM structural_cycles WHERE id = ?", (cycle_id,)).fetchone()
    if not row or not row["params_json"]:
        return {}
    try:
        return json.loads(row["params_json"])
    except json.JSONDecodeError:
        return {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-freeze",
        action="store_true",
        help="Только расчёт и отчёт в Sheets, без записи cycle_levels / trading_state.",
    )
    args = parser.parse_args()
    auto_freeze = not args.no_freeze

    exported_at = datetime.now(timezone.utc).isoformat()
    res = run_structural_pipeline(auto_freeze=auto_freeze)
    cycle_id = res.get("structural_cycle_id")
    if not cycle_id:
        print(json.dumps(res, ensure_ascii=False, indent=2))
        raise SystemExit("structural scan failed (no cycle_id)")

    conn = get_connection()
    cur = conn.cursor()
    pj = _load_params_json(cur, cycle_id)
    sc = cur.execute(
        """
        SELECT phase, pool_median_w, pool_mad, pool_k, symbols_valid_count
        FROM structural_cycles WHERE id = ?
        """,
        (cycle_id,),
    ).fetchone()

    rows = cur.execute(
        """
        SELECT
            symbol, status,
            level_below_id, level_above_id,
            L_price, U_price, atr, W_atr,
            ref_price_ws, mid_price, mid_band_low, mid_band_high,
            volume_peak_below, volume_peak_above,
            tier_below, tier_above
        FROM structural_cycle_symbols
        WHERE cycle_id = ?
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    conn.close()

    ref_src = res.get("ref_price_source")
    pool_m_w = float(res.get("pool_median_w") or 0.0)
    pool_mad_w = float(res.get("pool_mad") or 0.0)
    pool_m_r = float(res.get("pool_median_r") or 0.0)
    pool_mad_r = float(res.get("pool_mad_r") or 0.0)
    mad_k = float((sc["pool_k"] if sc and sc["pool_k"] is not None else (pj.get("mad_k") or 0.0)) or 0.0)
    center_k = float(pj.get("center_mad_k") or 0.0)
    center_enabled = bool(pj.get("center_filter_enabled"))
    records = []
    for r in rows:
        lp = r["L_price"]
        up = r["U_price"]
        atr = r["atr"]
        refp = r["ref_price_ws"]
        w_atr = r["W_atr"]
        center_ratio = (
            (float(refp) - float(lp)) / (float(up) - float(lp))
            if refp is not None and lp is not None and up is not None and float(up) > float(lp)
            else None
        )
        abs_w = abs(float(w_atr) - pool_m_w) if w_atr is not None else None
        z_w = (abs_w / pool_mad_w) if abs_w is not None and pool_mad_w > 1e-9 else None
        abs_r = abs(float(center_ratio) - pool_m_r) if center_ratio is not None else None
        z_r = (abs_r / pool_mad_r) if abs_r is not None and pool_mad_r > 1e-9 else None
        ok_w = (1 if z_w <= mad_k else 0) if z_w is not None else None
        ok_r = (1 if z_r <= center_k else 0) if (center_enabled and z_r is not None) else None
        if ok_w is None:
            tier_hint = None
        elif center_enabled and ok_r is not None:
            tier_hint = "ok_w_and_ok_r" if (ok_w == 1 and ok_r == 1) else ("ok_w_only" if ok_w == 1 else "fallback_any")
        else:
            tier_hint = "ok_w_only" if ok_w == 1 else "fallback_any"
        width_price = (float(up) - float(lp)) if lp is not None and up is not None else None
        dist_l = (abs(float(refp) - float(lp)) / float(atr)) if refp and lp and atr else None
        dist_u = (abs(float(up) - float(refp)) / float(atr)) if refp and up and atr else None
        records.append(
            {
                "exported_at_utc": exported_at,
                "structural_cycle_id": cycle_id,
                "ref_price_source": ref_src,
                "scan_phase": sc["phase"] if sc else None,
                "pool_median_W_atr": sc["pool_median_w"] if sc else None,
                "pool_MAD_W_atr": sc["pool_mad"] if sc else None,
                "pool_median_center_ratio": res.get("pool_median_r"),
                "pool_MAD_center_ratio": res.get("pool_mad_r"),
                "pool_MAD_k": sc["pool_k"] if sc else None,
                "center_MAD_k": pj.get("center_mad_k"),
                "center_filter_enabled": pj.get("center_filter_enabled"),
                "target_align_enabled": pj.get("target_align_enabled"),
                "anchor_symbols": ",".join(pj.get("anchor_symbols") or []),
                "target_w_band_k": pj.get("target_w_band_k"),
                "target_center_weight": pj.get("target_center_weight"),
                "target_width_weight": pj.get("target_width_weight"),
                "symbols_ok_in_pool": sc["symbols_valid_count"] if sc else None,
                "min_pool_required": pj.get("min_pool_symbols"),
                "allowed_level_types": ",".join(pj.get("allowed_level_types") or []),
                "symbol": r["symbol"],
                "row_status": r["status"],
                "ref_price": refp,
                "long_L_strongest_below": lp,
                "short_U_strongest_above": up,
                "level_below_id": r["level_below_id"],
                "level_above_id": r["level_above_id"],
                "volume_peak_below": r["volume_peak_below"],
                "volume_peak_above": r["volume_peak_above"],
                "tier_below": r["tier_below"],
                "tier_above": r["tier_above"],
                "atr_daily": atr,
                "corridor_width_atr": w_atr,
                "center_ratio_ref_in_corridor": center_ratio,
                "z_w": z_w,
                "z_r": z_r if center_enabled else None,
                "ok_w": ok_w,
                "ok_r": ok_r if center_enabled else None,
                "chosen_tier_hint": tier_hint,
                "corridor_width_price": width_price,
                "mid_price": r["mid_price"],
                "mid_band_low": r["mid_band_low"],
                "mid_band_high": r["mid_band_high"],
                "dist_ref_to_L_atr": dist_l,
                "dist_ref_to_U_atr": dist_u,
            }
        )

    df = pd.DataFrame(records)
    exporter = SheetsExporter(
        credentials_path=os.getenv("GOOGLE_CREDENTIALS_PATH", es.CREDENTIALS_PATH),
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or es.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or es.SHEET_ID,
    )
    ws = os.getenv("STRUCTURAL_LEVELS_REPORT_WORKSHEET", STRUCTURAL_LEVELS_REPORT_WORKSHEET)
    exporter.export_dataframe_to_sheet(df, es.SHEET_TITLE, ws)
    print(
        json.dumps(
            {
                "worksheet": ws,
                "rows": len(df),
                "structural_cycle_id": cycle_id,
                "pipeline": res,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
