"""
Regression Discontinuity Design (RDD) — causal/rdd.py

Question: Do prices have sharp discontinuities at end-of-month or pre-holiday thresholds?

Setup:
  - Running variable: days-to-month-end or day-of-year
  - Outcome: probability of item being on sale
  - Bandwidth: ±7 days around threshold
  - Method: local linear regression (rdrobust-style) or manual polynomial fit
  - Output: estimated jump size at each threshold

Usage:
    result = run_rdd_analysis(threshold_type="month_end")
"""

from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _get_conn():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="MARTS",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def load_rdd_data() -> pd.DataFrame:
    """Load price history with sale flag for RDD analysis."""
    conn = _get_conn()
    sql = """
        SELECT item_id, store_id, price_date, regular_price, promo_price, is_on_sale
        FROM MARTS.MART_PRICE_HISTORY
        WHERE price_date >= DATEADD('year', -2, CURRENT_DATE())
          AND regular_price IS NOT NULL
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
    finally:
        conn.close()

    df.columns = [c.lower() for c in df.columns]
    df["price_date"] = pd.to_datetime(df["price_date"])
    return df


def _add_running_variable(df: pd.DataFrame, threshold_type: str) -> pd.DataFrame:
    """Add running variable and treatment indicator for a given threshold type."""
    df = df.copy()
    if threshold_type == "month_end":
        # Days to end of month (negative = before, positive = after)
        import calendar
        df["days_to_month_end"] = df["price_date"].apply(
            lambda d: d.day - calendar.monthrange(d.year, d.month)[1]
        )
        df["running_var"] = df["days_to_month_end"]
    elif threshold_type == "day_of_year":
        df["running_var"] = df["price_date"].dt.dayofyear - 182  # centered at mid-year
    else:
        raise ValueError(f"Unknown threshold_type: {threshold_type}")
    return df


def local_linear_rdd(
    df: pd.DataFrame,
    running_col: str = "running_var",
    outcome_col: str = "is_on_sale",
    bandwidth: int = 7,
) -> dict[str, float]:
    """
    Estimate the RDD discontinuity using local linear regression.

    Returns dict with: jump_estimate, pvalue, bandwidth, n_left, n_right
    """
    import statsmodels.formula.api as smf

    df_bw = df[df[running_col].abs() <= bandwidth].copy()
    df_bw["above_threshold"] = (df_bw[running_col] >= 0).astype(int)
    df_bw[outcome_col] = df_bw[outcome_col].astype(float)

    formula = f"{outcome_col} ~ {running_col} * above_threshold"
    try:
        model = smf.ols(formula=formula, data=df_bw).fit(cov_type="HC3")
        jump = model.params.get("above_threshold", np.nan)
        pval = model.pvalues.get("above_threshold", np.nan)
        return {
            "jump_estimate": round(float(jump), 6),
            "pvalue": round(float(pval), 6),
            "bandwidth": bandwidth,
            "n_left": int((df_bw[running_col] < 0).sum()),
            "n_right": int((df_bw[running_col] >= 0).sum()),
        }
    except Exception:
        logger.exception("Local linear RDD failed")
        return {}


def run_rdd_analysis(threshold_type: str = "month_end") -> dict[str, Any]:
    """Run full RDD pipeline and return discontinuity estimates."""
    df = load_rdd_data()
    if df.empty:
        logger.warning("No data for RDD analysis")
        return {}

    df = _add_running_variable(df, threshold_type)
    result = local_linear_rdd(df)
    result["threshold_type"] = threshold_type
    result["n_total"] = len(df)
    logger.info(
        "RDD result: threshold=%s jump=%.4f p=%.4f",
        threshold_type,
        result.get("jump_estimate", np.nan),
        result.get("pvalue", np.nan),
    )
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for t in ["month_end", "day_of_year"]:
        r = run_rdd_analysis(threshold_type=t)
        print(f"RDD ({t}):", r)
