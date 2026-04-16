from __future__ import annotations
from fastapi import APIRouter, Query
from backend.database import db_cursor

router = APIRouter()


@router.get("/items")
def list_items() -> list[dict]:
    """Return all distinct items tracked."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT item_id, item_name, category
            FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
            ORDER BY item_name
            LIMIT 500
        """)
        rows = cur.fetchall()
    return [
        {"item_id": r["ITEM_ID"], "item_name": r["ITEM_NAME"], "category": r["CATEGORY"]}
        for r in rows
    ]


@router.get("/stores")
def list_stores() -> list[dict]:
    """Return all distinct stores tracked."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT store_id
            FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
            ORDER BY store_id
        """)
        rows = cur.fetchall()
    return [{"store_id": r["STORE_ID"]} for r in rows]


@router.get("/history")
def price_history(
    item_id: str = Query(...),
    store_id: str = Query(...),
    days: int = Query(90, ge=7, le=365),
) -> list[dict]:
    """Price history for a given item × store over N days."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                price_date, regular_price, promo_price, is_on_sale,
                price_delta_7d, price_delta_30d, price_vs_90d_avg
            FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
            WHERE item_id  = %s
              AND store_id = %s
              AND price_date >= DATEADD('day', -%s, CURRENT_DATE())
            ORDER BY price_date
        """, (item_id, store_id, days))
        rows = cur.fetchall()
    return [
        {
            "date":            str(r["PRICE_DATE"]),
            "regular_price":   float(r["REGULAR_PRICE"] or 0),
            "promo_price":     float(r["PROMO_PRICE"]) if r["PROMO_PRICE"] else None,
            "is_on_sale":      bool(r["IS_ON_SALE"]),
            "delta_7d":        float(r["PRICE_DELTA_7D"]) if r["PRICE_DELTA_7D"] else None,
            "delta_30d":       float(r["PRICE_DELTA_30D"]) if r["PRICE_DELTA_30D"] else None,
            "vs_90d_avg":      float(r["PRICE_VS_90D_AVG"]) if r["PRICE_VS_90D_AVG"] else None,
        }
        for r in rows
    ]


@router.get("/categories")
def list_categories() -> list[str]:
    with db_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT category
            FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
            WHERE category IS NOT NULL
            ORDER BY category
        """)
        return [r["CATEGORY"] for r in cur.fetchall()]
