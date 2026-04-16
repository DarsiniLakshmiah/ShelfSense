from __future__ import annotations
from fastapi import APIRouter, Query
from backend.database import db_cursor

router = APIRouter()


@router.get("")
def get_recommendations(
    recommendation: str | None = Query(None),
    limit: int = Query(100, le=500),
) -> list[dict]:
    """Latest buy-now signals, optionally filtered by recommendation type."""
    with db_cursor() as cur:
        if recommendation:
            cur.execute("""
                SELECT
                    item_id, store_id, signal_date, regular_price,
                    predicted_price_7d, predicted_price_14d,
                    buy_now_probability, recommendation, model_version
                FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS
                WHERE signal_date = (SELECT MAX(signal_date) FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS)
                  AND recommendation = %s
                ORDER BY buy_now_probability DESC NULLS LAST
                LIMIT %s
            """, (recommendation, limit))
        else:
            cur.execute("""
                SELECT
                    item_id, store_id, signal_date, regular_price,
                    predicted_price_7d, predicted_price_14d,
                    buy_now_probability, recommendation, model_version
                FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS
                WHERE signal_date = (SELECT MAX(signal_date) FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS)
                ORDER BY buy_now_probability DESC NULLS LAST
                LIMIT %s
            """, (limit,))
        rows = cur.fetchall()
    return [
        {
            "item_id":            r["ITEM_ID"],
            "store_id":           r["STORE_ID"],
            "signal_date":        str(r["SIGNAL_DATE"]),
            "regular_price":      float(r["REGULAR_PRICE"] or 0),
            "predicted_7d":       float(r["PREDICTED_PRICE_7D"]) if r["PREDICTED_PRICE_7D"] else None,
            "predicted_14d":      float(r["PREDICTED_PRICE_14D"]) if r["PREDICTED_PRICE_14D"] else None,
            "probability":        float(r["BUY_NOW_PROBABILITY"] or 0),
            "recommendation":     r["RECOMMENDATION"],
            "model_version":      r["MODEL_VERSION"],
        }
        for r in rows
    ]
