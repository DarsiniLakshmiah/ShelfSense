from __future__ import annotations
from fastapi import APIRouter
from backend.database import db_cursor

router = APIRouter()


@router.get("")
def get_summary() -> dict:
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(DISTINCT item_id)  AS total_items,
                COUNT(DISTINCT store_id) AS total_stores,
                MAX(price_date)          AS last_updated
            FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
        """)
        row = cur.fetchone() or {}

        cur.execute("""
            SELECT
                SUM(CASE WHEN recommendation = 'BUY_NOW' THEN 1 ELSE 0 END) AS buy_now_count,
                SUM(CASE WHEN recommendation = 'WAIT'    THEN 1 ELSE 0 END) AS wait_count,
                SUM(CASE WHEN recommendation = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral_count
            FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS
            WHERE signal_date = (SELECT MAX(signal_date) FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS)
        """)
        signals = cur.fetchone() or {}

    return {
        "total_items":   row.get("TOTAL_ITEMS", 0),
        "total_stores":  row.get("TOTAL_STORES", 0),
        "last_updated":  str(row.get("LAST_UPDATED", "")),
        "buy_now_count": signals.get("BUY_NOW_COUNT", 0),
        "wait_count":    signals.get("WAIT_COUNT", 0),
        "neutral_count": signals.get("NEUTRAL_COUNT", 0),
    }
