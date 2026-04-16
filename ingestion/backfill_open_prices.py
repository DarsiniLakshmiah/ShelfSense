"""
Historical price backfill using Open Food Facts Open Prices API.

Pulls crowdsourced US price history for every UPC already tracked in
KROGER_PRICES_RAW and writes normalized records back into that same table
with the original price dates as `loaded_at`.  This lets the existing
dbt → ML pipeline treat the data as if the Kroger DAG had been running
for months.

Usage:
    python ingestion/backfill_open_prices.py              # last 180 days
    python ingestion/backfill_open_prices.py --days 365   # last year
    python ingestion/backfill_open_prices.py --dry-run    # fetch only, no write
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

STORE_ID = "01100002"          # store to attribute backfilled rows to
PAGE_SIZE = 100                # Open Prices max page size
MAX_PAGES_PER_UPC = 5         # cap per UPC  (500 records max)
RATE_LIMIT_SLEEP = 0.3        # seconds between API calls


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def _get_conn() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _load_tracked_upcs() -> list[str]:
    """Return distinct UPCs already in KROGER_PRICES_RAW."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT product_id
            FROM RAW.KROGER_PRICES_RAW
            WHERE product_id IS NOT NULL AND product_id != ''
            ORDER BY product_id
        """)
        upcs = [row[0] for row in cur.fetchall()]
        logger.info("Found %d distinct UPCs in KROGER_PRICES_RAW", len(upcs))
        return upcs
    finally:
        conn.close()


def _load_existing_dates() -> set[tuple[str, str]]:
    """Return (product_id, date) pairs that already exist to avoid duplicates."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT product_id, TO_CHAR(loaded_at::DATE, 'YYYY-MM-DD')
            FROM RAW.KROGER_PRICES_RAW
            WHERE loaded_at::DATE < CURRENT_DATE()
        """)
        existing = {(r[0], r[1]) for r in cur.fetchall()}
        logger.info("Found %d existing (product, date) pairs — skipping these", len(existing))
        return existing
    finally:
        conn.close()


def _bulk_insert(rows: list[dict], dry_run: bool = False) -> int:
    """Write a batch of normalized rows to KROGER_PRICES_RAW."""
    if not rows:
        return 0
    if dry_run:
        logger.info("[DRY RUN] Would insert %d rows", len(rows))
        return 0

    df = pd.DataFrame(rows)

    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS KROGER_BACKFILL_STAGE (
                STORE_ID        VARCHAR,
                PRODUCT_ID      VARCHAR,
                LOADED_AT       VARCHAR,
                RAW_PAYLOAD_STR VARCHAR
            )
        """)
        cur.execute("TRUNCATE TABLE KROGER_BACKFILL_STAGE")

        success, _, staged, _ = write_pandas(
            conn, df, "KROGER_BACKFILL_STAGE", auto_create_table=False
        )
        if not success:
            raise RuntimeError("write_pandas failed during backfill staging")

        cur.execute("""
            INSERT INTO RAW.KROGER_PRICES_RAW (store_id, product_id, loaded_at, raw_payload)
            SELECT
                STORE_ID,
                PRODUCT_ID,
                TO_TIMESTAMP_NTZ(LOADED_AT),
                PARSE_JSON(RAW_PAYLOAD_STR)
            FROM KROGER_BACKFILL_STAGE
        """)
        conn.commit()
        inserted = cur.rowcount
        logger.info("Inserted %d backfill rows into KROGER_PRICES_RAW", inserted)
        return inserted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Open Prices helpers
# ---------------------------------------------------------------------------

def _fetch_upc_history(
    upc: str,
    cutoff_date: str,
) -> list[dict]:
    """
    Fetch all Open Prices records for a UPC after cutoff_date.
    Returns raw API item dicts.
    """
    import requests

    base_url = "https://prices.openfoodfacts.org/api/v1/prices"
    results: list[dict] = []

    for page in range(1, MAX_PAGES_PER_UPC + 1):
        try:
            resp = requests.get(
                base_url,
                params={
                    "product_code": upc,
                    "country": "us",
                    "page": page,
                    "size": PAGE_SIZE,
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Open Prices request failed for UPC %s page %d: %s", upc, page, exc)
            break

        items = data.get("items", [])
        for item in items:
            date_str = item.get("date") or item.get("created_at", "")[:10]
            if date_str >= cutoff_date:
                results.append(item)

        # Stop paginating if last page or all remaining are too old
        if len(items) < PAGE_SIZE:
            break

        time.sleep(RATE_LIMIT_SLEEP)

    return results


def _normalize_to_kroger(item: dict, upc: str) -> dict:
    """
    Convert an Open Prices record to a Kroger-compatible JSON payload.
    Matches the field paths used in stg_kroger_prices.sql.
    """
    price = item.get("price") or 0.0
    is_discounted = item.get("price_is_discounted", False)
    price_without_discount = item.get("price_without_discount")

    regular_price = float(price_without_discount) if (is_discounted and price_without_discount) else float(price)
    promo_price = float(price) if is_discounted else None

    return {
        "productId": upc,
        "description": item.get("product_name") or item.get("product", {}).get("product_name", ""),
        "upc": upc,
        "categories": [item.get("category") or "grocery"],
        "items": [
            {
                "price": {
                    "regular": round(regular_price, 2),
                    **({"promo": round(promo_price, 2)} if promo_price else {}),
                },
                "size": "",
                "soldBy": "UNIT",
            }
        ],
        "_source": "open_prices_backfill",
        "_location_id": STORE_ID,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_backfill(days: int = 180, dry_run: bool = False, batch_size: int = 500) -> None:
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    logger.info("Backfilling Open Prices data from %s onward (dry_run=%s)", cutoff, dry_run)

    upcs = _load_tracked_upcs()
    if not upcs:
        logger.error("No UPCs found in KROGER_PRICES_RAW — run the daily DAG first")
        return

    existing = _load_existing_dates()

    total_inserted = 0
    batch: list[dict] = []

    for i, upc in enumerate(upcs, 1):
        logger.info("[%d/%d] Fetching history for UPC %s", i, len(upcs), upc)
        items = _fetch_upc_history(upc, cutoff)

        for item in items:
            date_str = (item.get("date") or item.get("created_at", ""))[:10]
            if not date_str or date_str < cutoff:
                continue
            if (upc, date_str) in existing:
                continue  # already have this day's price for this product

            payload = _normalize_to_kroger(item, upc)
            batch.append({
                "STORE_ID": STORE_ID,
                "PRODUCT_ID": upc,
                "LOADED_AT": f"{date_str} 08:00:00",  # attribute to 8am on that date
                "RAW_PAYLOAD_STR": json.dumps(payload),
            })

        # Flush batch
        if len(batch) >= batch_size:
            total_inserted += _bulk_insert(batch, dry_run=dry_run)
            batch = []

        time.sleep(RATE_LIMIT_SLEEP)

    # Final flush
    if batch:
        total_inserted += _bulk_insert(batch, dry_run=dry_run)

    logger.info("Backfill complete. Total rows inserted: %d", total_inserted)

    if total_inserted > 0 and not dry_run:
        logger.info(
            "Next steps:\n"
            "  1. cd dbt && dbt run --select stg_kroger_prices mart_price_history mart_promo_calendar --profiles-dir .\n"
            "  2. python ml/classifier.py\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical prices from Open Food Facts Open Prices")
    parser.add_argument("--days", type=int, default=180, help="How many days of history to backfill (default 180)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but do not write to Snowflake")
    args = parser.parse_args()

    run_backfill(days=args.days, dry_run=args.dry_run)
