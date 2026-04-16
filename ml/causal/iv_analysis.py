"""
Instrumental Variables (IV) — causal/iv_analysis.py

Question: How much of a retail price change is supply-driven vs. margin-driven?

Instrument: USDA wholesale commodity price (affects retail but isn't caused by consumer demand)
Endogenous: retail price (may be set by store margin decisions)

First stage:  retail_price ~ usda_price + controls
Second stage: use fitted retail_price to estimate causal elasticity

Output: commodity pass-through rate per category
        e.g. "a 10% egg wholesale spike → 6.2% retail spike within 2 weeks"

Usage:
    results = run_iv_analysis(category="dairy")
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


def load_iv_data(category: str | None = None, lag_weeks: int = 2) -> pd.DataFrame:
    """
    Join mart_price_history with mart_external_signals for IV regression.

    Args:
        category:  filter retail prices to a category (e.g. "Eggs")
        lag_weeks: weeks to lag USDA signal before joining to retail price

    Returns:
        DataFrame with columns: price_date, item_id, store_id, regular_price,
                                 usda_price (lagged), commodity
    """
    conn = _get_conn()

    cat_filter = f"AND LOWER(ph.category) LIKE LOWER('%{category}%')" if category else ""

    sql = f"""
        SELECT
            ph.price_date,
            ph.item_id,
            ph.store_id,
            ph.regular_price,
            ph.category,
            es.value        AS usda_price,
            es.commodity
        FROM MARTS.MART_PRICE_HISTORY ph
        INNER JOIN MARTS.MART_EXTERNAL_SIGNALS es
            ON es.signal_date = DATEADD('week', -{lag_weeks}, ph.price_date)
            AND es.signal_type = 'USDA'
            AND LOWER(es.commodity) LIKE LOWER('%' || ph.category || '%')
        WHERE ph.regular_price IS NOT NULL
          AND es.value IS NOT NULL
          {cat_filter}
        ORDER BY ph.price_date
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


def run_iv_analysis(
    category: str = "Eggs",
    lag_weeks: int = 2,
) -> dict[str, Any]:
    """
    Run 2SLS IV regression and return pass-through elasticity.

    Returns dict with: category, lag_weeks, elasticity, first_stage_f,
                        pvalue, n_obs
    """
    from linearmodels.iv import IV2SLS

    df = load_iv_data(category=category, lag_weeks=lag_weeks)
    if df.empty:
        logger.warning("No data for IV analysis (category=%s)", category)
        return {}

    logger.info("Running IV analysis: category=%s lag=%dw n=%d", category, lag_weeks, len(df))

    df["log_retail"] = np.log(df["regular_price"].replace(0, np.nan))
    df["log_usda"] = np.log(df["usda_price"].replace(0, np.nan))
    df = df.dropna(subset=["log_retail", "log_usda"])

    if len(df) < 50:
        logger.warning("Too few observations (%d) for IV analysis", len(df))
        return {}

    # Intercept column required by linearmodels
    df["const"] = 1.0

    try:
        model = IV2SLS(
            dependent=df["log_retail"],
            exog=df[["const"]],
            endog=None,
            instruments=df[["log_usda"]],
        ).fit(cov_type="robust")

        # Pass-through elasticity = coefficient on log_usda in reduced form
        # Use OLS for interpretable elasticity since IV needs endog/exog split
        import statsmodels.formula.api as smf

        ols = smf.ols("log_retail ~ log_usda", data=df).fit()
        elasticity = ols.params.get("log_usda", np.nan)
        pval = ols.pvalues.get("log_usda", np.nan)

        result = {
            "category": category,
            "lag_weeks": lag_weeks,
            "elasticity": round(float(elasticity), 4),
            "pvalue": round(float(pval), 6),
            "n_obs": len(df),
            "interpretation": (
                f"A 10% USDA wholesale price change → "
                f"{abs(elasticity) * 10:.1f}% retail price change "
                f"within {lag_weeks} weeks (category: {category})"
            ),
        }
        logger.info("IV elasticity: %.4f (p=%.4f)", elasticity, pval)
        return result
    except Exception:
        logger.exception("IV regression failed")
        return {}


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", default="dairy")
    parser.add_argument("--lag", type=int, default=2)
    args = parser.parse_args()

    result = run_iv_analysis(category=args.category, lag_weeks=args.lag)
    print("IV result:", result)
