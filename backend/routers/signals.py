from __future__ import annotations
from fastapi import APIRouter, Query
from backend.database import db_cursor

router = APIRouter()


@router.get("")
def get_signals(
    days: int = Query(90, ge=7, le=365),
    signal_type: str | None = Query(None),
) -> list[dict]:
    """External signals (USDA / BLS / NOAA) over N days."""
    with db_cursor() as cur:
        if signal_type:
            cur.execute("""
                SELECT signal_date, signal_type, commodity, value, yoy_change_pct
                FROM SHELFSENSE.MARTS.MART_EXTERNAL_SIGNALS
                WHERE signal_date >= DATEADD('day', -%s, CURRENT_DATE())
                  AND signal_type = %s
                ORDER BY signal_date
            """, (days, signal_type))
        else:
            cur.execute("""
                SELECT signal_date, signal_type, commodity, value, yoy_change_pct
                FROM SHELFSENSE.MARTS.MART_EXTERNAL_SIGNALS
                WHERE signal_date >= DATEADD('day', -%s, CURRENT_DATE())
                ORDER BY signal_date
            """, (days,))
        rows = cur.fetchall()
    return [
        {
            "date":           str(r["SIGNAL_DATE"]),
            "signal_type":    r["SIGNAL_TYPE"],
            "commodity":      r["COMMODITY"],
            "value":          float(r["VALUE"]) if r["VALUE"] else None,
            "yoy_change_pct": float(r["YOY_CHANGE_PCT"]) if r["YOY_CHANGE_PCT"] else None,
        }
        for r in rows
    ]
