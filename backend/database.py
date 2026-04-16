"""Snowflake connection helper for FastAPI backend."""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


@contextmanager
def db_cursor() -> Generator:
    conn = get_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        yield cur
    finally:
        conn.close()
