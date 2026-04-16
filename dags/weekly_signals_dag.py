"""
Weekly Signals DAG — runs every Tuesday at 9 AM (after USDA weekly release).

Steps:
  1. Pull USDA commodity prices
  2. Pull BLS series updates
  3. Pull NOAA weather anomalies (past 7 days)
  4. Write all to RAW.EXTERNAL_SIGNALS_RAW
  5. Run dbt staging + mart_external_signals
  6. Re-run causal inference: DiD + IV
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)


def _get_snowflake_conn():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _insert_external_signals(source: str, records: list) -> None:
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    if not records:
        logger.info("No records to insert for source=%s", source)
        return

    staging_df = pd.DataFrame(
        [{"SOURCE": source, "RAW_PAYLOAD_STR": json.dumps(r)} for r in records]
    )

    conn = _get_snowflake_conn()
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS EXTERNAL_SIGNALS_STAGE (
                SOURCE          VARCHAR,
                RAW_PAYLOAD_STR VARCHAR
            )
        """)
        cur.execute("TRUNCATE TABLE EXTERNAL_SIGNALS_STAGE")

        success, num_chunks, num_rows, _ = write_pandas(
            conn, staging_df, "EXTERNAL_SIGNALS_STAGE", auto_create_table=False
        )
        logger.info("write_pandas: success=%s chunks=%d rows_staged=%d", success, num_chunks, num_rows)
        if not success:
            raise RuntimeError(f"write_pandas failed to stage rows for source={source}")

        cur.execute("""
            INSERT INTO RAW.EXTERNAL_SIGNALS_RAW (source, raw_payload)
            SELECT SOURCE, PARSE_JSON(RAW_PAYLOAD_STR)
            FROM EXTERNAL_SIGNALS_STAGE
        """)
        conn.commit()
        inserted = cur.rowcount
        logger.info("Inserted %d rows for source=%s", inserted, source)
    finally:
        conn.close()


def pull_usda_signals(**context: dict) -> None:
    from dotenv import load_dotenv

    load_dotenv()
    from ingestion.usda_client import USDAClient

    client = USDAClient()
    year = datetime.utcnow().year
    commodities = ["EGGS", "BROILERS", "CATTLE", "MILK"]
    all_records: list[dict] = []
    for commodity in commodities:
        # Query one year at a time — NASS API times out on large multi-year requests
        for y in [year - 1, year]:
            try:
                records = client.get_commodity_prices(commodity, y, y)
                all_records.extend(records)
                logger.info("USDA %s %d: %d records", commodity, y, len(records))
            except Exception:
                logger.exception("USDA fetch failed for commodity=%s year=%d", commodity, y)

    _insert_external_signals("USDA", all_records)


def pull_bls_signals(**context: dict) -> None:
    from dotenv import load_dotenv

    load_dotenv()
    from ingestion.bls_client import BLSClient

    client = BLSClient()
    year = datetime.utcnow().year
    series_list = client.get_default_series(str(year - 1), str(year))
    flat_records = client.flatten_series(series_list)
    _insert_external_signals("BLS", flat_records)


def pull_noaa_signals(**context: dict) -> None:
    from dotenv import load_dotenv

    load_dotenv()
    from ingestion.noaa_client import NOAAClient

    client = NOAAClient()
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    # Pull for major agricultural states
    ag_states = [
        "FIPS:06",  # California (produce)
        "FIPS:19",  # Iowa (eggs/pork)
        "FIPS:20",  # Kansas (wheat/beef)
        "FIPS:48",  # Texas (beef)
    ]
    all_records: list[dict] = []
    for state_fips in ag_states:
        try:
            records = client.get_weather_anomalies(start_date, end_date, location_id=state_fips)
            all_records.extend(records)
        except Exception:
            logger.exception("NOAA fetch failed for location=%s", state_fips)

    _insert_external_signals("NOAA", all_records)


def run_dbt_external_signals(**context: dict) -> None:
    import subprocess

    result = subprocess.run(
        [
            "dbt", "run",
            "--select", "stg_usda_commodities stg_noaa_weather stg_bls_series mart_external_signals",
            "--profiles-dir", "/opt/airflow/dbt",
        ],
        cwd="/opt/airflow/dbt",
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt external signals run failed")


def run_causal_analysis(**context: dict) -> None:
    from ml.causal.diff_in_diff import run_did_analysis
    from ml.causal.iv_analysis import run_iv_analysis

    run_did_analysis()
    run_iv_analysis()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "shelfsense",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="weekly_signals",
    description="USDA/BLS/NOAA ingestion → dbt → causal inference",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 9 * * 2",  # Tuesdays 9 AM
    default_args=default_args,
    catchup=False,
    tags=["shelfsense", "signals"],
) as dag:

    usda_task = PythonOperator(task_id="pull_usda", python_callable=pull_usda_signals)
    bls_task = PythonOperator(task_id="pull_bls", python_callable=pull_bls_signals)
    noaa_task = PythonOperator(task_id="pull_noaa", python_callable=pull_noaa_signals)
    dbt_task = PythonOperator(task_id="dbt_external_signals", python_callable=run_dbt_external_signals)
    causal_task = PythonOperator(task_id="causal_analysis", python_callable=run_causal_analysis)

    [usda_task, bls_task, noaa_task] >> dbt_task >> causal_task
