"""
Daily Prices DAG — runs every day at 7 AM.

Steps:
  1. Refresh Kroger OAuth token (implicit in KrogerClient)
  2. Fetch product prices for 500 tracked items × 3 stores
  3. Write raw JSON to Snowflake RAW.KROGER_PRICES_RAW
  4. Run dbt staging: stg_kroger_prices
  5. Run dbt marts: mart_price_history, mart_promo_calendar
  6. Run ML classifier → write to mart_buy_signals
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Allow imports from project root when running inside Docker
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TRACKED_TERMS: list[str] = [
    "eggs",
    "milk",
    "butter",
    "chicken breast",
    "ground beef",
    "bread",
    "orange juice",
    "cheddar cheese",
    "bacon",
    "yogurt",
    "apple",
    "banana",
    "broccoli",
    "pasta",
    "rice",
]

# Store locations — fill in real IDs from kroger_client.get_locations()
# These are examples; the init task discovers them dynamically.
DEFAULT_ZIP_CODES: list[str] = ["10001", "90210", "60601"]

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def fetch_and_store_kroger_prices(**context: dict) -> None:
    """Fetch Kroger prices and write raw JSON to Snowflake."""
    from dotenv import load_dotenv

    load_dotenv()

    import snowflake.connector
    from ingestion.kroger_client import KrogerClient

    client = KrogerClient()

    # Discover store IDs from ZIP codes and persist metadata
    location_ids: list[str] = []
    discovered_stores: list[dict] = []
    for zip_code in DEFAULT_ZIP_CODES:
        try:
            locations = client.get_locations(zip_code, limit=1)
            if locations:
                loc = locations[0]
                location_ids.append(loc["locationId"])
                addr = loc.get("address", {})
                discovered_stores.append({
                    "store_id": loc["locationId"],
                    "store_name": loc.get("name", ""),
                    "store_zip": addr.get("zipCode", ""),
                    "store_state": addr.get("state", ""),
                    "store_city": addr.get("city", ""),
                })
                logger.info("Store for zip %s: %s (%s, %s)", zip_code, loc["locationId"], addr.get("city"), addr.get("state"))
        except Exception:
            logger.exception("Failed to get location for zip=%s", zip_code)

    if not location_ids:
        raise RuntimeError("No Kroger store locations found")

    # Upsert store metadata into RAW.KROGER_STORE_LOOKUP via write_pandas + MERGE
    # Avoids binding %s inside USING (SELECT ...) which is unreliable in some connector versions.
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    conn_meta = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        cur_meta = conn_meta.cursor()
        stores_df = pd.DataFrame(discovered_stores).rename(columns=str.upper)
        cur_meta.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS KROGER_STORE_STAGE (
                STORE_ID    VARCHAR,
                STORE_NAME  VARCHAR,
                STORE_ZIP   VARCHAR,
                STORE_STATE VARCHAR,
                STORE_CITY  VARCHAR
            )
        """)
        cur_meta.execute("TRUNCATE TABLE KROGER_STORE_STAGE")
        write_pandas(conn_meta, stores_df, "KROGER_STORE_STAGE", auto_create_table=False)
        cur_meta.execute("""
            MERGE INTO RAW.KROGER_STORE_LOOKUP AS tgt
            USING KROGER_STORE_STAGE AS src
            ON tgt.store_id = src.store_id
            WHEN MATCHED THEN UPDATE SET
                store_name  = src.store_name,
                store_zip   = src.store_zip,
                store_state = src.store_state,
                store_city  = src.store_city,
                _updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (store_id, store_name, store_zip, store_state, store_city)
            VALUES (src.store_id, src.store_name, src.store_zip, src.store_state, src.store_city)
        """)
        conn_meta.commit()
        logger.info("Upserted %d store(s) into RAW.KROGER_STORE_LOOKUP", len(discovered_stores))
    finally:
        conn_meta.close()

    # Fetch products
    all_products = client.get_all_tracked_products(TRACKED_TERMS, location_ids)
    logger.info("Total product records fetched: %d", len(all_products))

    # Write to Snowflake — bulk load via write_pandas (PUT + COPY INTO)
    # This avoids one network round-trip per row which causes SIGTERM timeouts.
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    staging_df = pd.DataFrame(
        [
            {
                "STORE_ID": p.get("_location_id", ""),
                "PRODUCT_ID": p.get("productId", ""),
                "RAW_PAYLOAD_STR": json.dumps(p),
            }
            for p in all_products
        ]
    )
    logger.info("Bulk-loading %d rows into Snowflake", len(staging_df))

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        cur = conn.cursor()

        # Stage into a temp VARCHAR table (write_pandas cannot write VARIANT directly)
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS KROGER_PRICES_STAGE (
                STORE_ID        VARCHAR,
                PRODUCT_ID      VARCHAR,
                RAW_PAYLOAD_STR VARCHAR
            )
        """)
        cur.execute("TRUNCATE TABLE KROGER_PRICES_STAGE")

        success, num_chunks, num_rows, _ = write_pandas(
            conn, staging_df, "KROGER_PRICES_STAGE", auto_create_table=False
        )
        logger.info("write_pandas: success=%s chunks=%d rows_staged=%d", success, num_chunks, num_rows)
        if not success:
            raise RuntimeError("write_pandas failed to stage rows into KROGER_PRICES_STAGE")

        # Single INSERT...SELECT converts VARCHAR → VARIANT in one statement
        cur.execute("""
            INSERT INTO RAW.KROGER_PRICES_RAW (store_id, product_id, raw_payload)
            SELECT STORE_ID, PRODUCT_ID, PARSE_JSON(RAW_PAYLOAD_STR)
            FROM KROGER_PRICES_STAGE
        """)
        conn.commit()
        inserted = cur.rowcount
        logger.info("Inserted %d rows into RAW.KROGER_PRICES_RAW", inserted)
    finally:
        conn.close()


def run_dbt_staging(**context: dict) -> None:
    """Run dbt staging models."""
    import subprocess

    result = subprocess.run(
        ["dbt", "run", "--select", "staging", "--profiles-dir", "/opt/airflow/dbt"],
        cwd="/opt/airflow/dbt",
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt staging run failed")


def run_dbt_marts(**context: dict) -> None:
    """Run dbt mart models (price_history, promo_calendar)."""
    import subprocess

    result = subprocess.run(
        [
            "dbt", "run",
            "--select", "mart_price_history mart_promo_calendar",
            "--profiles-dir", "/opt/airflow/dbt",
        ],
        cwd="/opt/airflow/dbt",
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt marts run failed")


def run_ml_classifier(**context: dict) -> None:
    """Run buy-now classifier and write results to mart_buy_signals."""
    from ml.classifier import BuyNowClassifier

    clf = BuyNowClassifier()
    clf.run_inference_and_write()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "shelfsense",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_prices",
    description="Fetch Kroger prices → Snowflake → dbt → ML buy signals",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 7 * * *",  # 7 AM daily
    default_args=default_args,
    catchup=False,
    tags=["shelfsense", "ingestion"],
) as dag:

    fetch_prices = PythonOperator(
        task_id="fetch_kroger_prices",
        python_callable=fetch_and_store_kroger_prices,
    )

    dbt_staging = PythonOperator(
        task_id="dbt_staging",
        python_callable=run_dbt_staging,
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=run_dbt_marts,
    )

    ml_signals = PythonOperator(
        task_id="ml_buy_signals",
        python_callable=run_ml_classifier,
        execution_timeout=timedelta(minutes=15),
    )

    fetch_prices >> dbt_staging >> dbt_marts >> ml_signals
