"""
Difference-in-Differences (DiD) — causal/diff_in_diff.py

Question: What is the causal effect of a commodity supply shock on retail prices?

Setup:
  - Treatment group: stores in states affected by a supply shock
  - Control group:   stores in unaffected states
  - Pre/post window: 4 weeks before and after shock event
  - Model: price ~ treatment + post + treatment*post + store_FE + time_FE
  - Output: ATT (average treatment effect on the treated) per shock event

Usage:
    run_did_analysis(event="bird_flu_2024", treated_states=["IA","MO"])
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
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


def load_did_data(
    event_date: str,
    treated_states: list[str],
    item_category: str | None = None,
    window_weeks: int = 4,
) -> pd.DataFrame:
    """
    Load pre/post price data for treated and control stores around an event.

    Args:
        event_date: ISO date string "YYYY-MM-DD" when shock occurred
        treated_states: list of 2-letter state codes for treatment group
        item_category: filter to a specific category (e.g. "Eggs")
        window_weeks: weeks before and after event to include

    Returns:
        DataFrame with columns: item_id, store_id, store_state, price_date,
                                 regular_price, treatment, post
    """
    conn = _get_conn()
    ev = datetime.strptime(event_date, "%Y-%m-%d")
    start = (ev - timedelta(weeks=window_weeks)).strftime("%Y-%m-%d")
    end = (ev + timedelta(weeks=window_weeks)).strftime("%Y-%m-%d")

    state_list = ", ".join(f"'{s}'" for s in treated_states)
    category_filter = f"AND LOWER(category) LIKE LOWER('%{item_category}%')" if item_category else ""

    sql = f"""
        SELECT
            item_id, store_id, store_state, price_date, regular_price, category
        FROM MARTS.MART_PRICE_HISTORY
        WHERE price_date BETWEEN '{start}' AND '{end}'
          {category_filter}
          AND regular_price IS NOT NULL
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
    finally:
        conn.close()

    if df.empty:
        logger.warning("No data returned for DiD analysis (event=%s)", event_date)
        return df

    df.columns = [c.lower() for c in df.columns]
    df["price_date"] = pd.to_datetime(df["price_date"])
    df["treatment"] = df["store_state"].isin(treated_states).astype(int)
    df["post"] = (df["price_date"] >= ev).astype(int)
    df["treatment_x_post"] = df["treatment"] * df["post"]
    return df


def run_did_analysis(
    event: str = "bird_flu_2024",
    event_date: str = "2024-02-01",
    treated_states: list[str] | None = None,
    item_category: str = "Eggs",
    window_weeks: int = 4,
) -> dict[str, Any]:
    """
    Run DiD regression and return ATT estimate.

    Returns:
        dict with keys: att, pvalue, conf_int_lower, conf_int_upper, n_obs
    """
    import statsmodels.formula.api as smf

    if treated_states is None:
        treated_states = ["IA", "MO", "MN"]  # states affected by 2024 bird flu

    df = load_did_data(event_date, treated_states, item_category, window_weeks)
    if df.empty:
        logger.warning("DiD analysis skipped — no data")
        return {}

    logger.info(
        "Running DiD: event=%s treated_states=%s n=%d", event, treated_states, len(df)
    )

    # Add store and time fixed effects via C() notation in statsmodels
    formula = "regular_price ~ treatment + post + treatment_x_post + C(store_id) + C(price_date)"
    try:
        model = smf.ols(formula=formula, data=df).fit(cov_type="HC1")
        att = model.params.get("treatment_x_post", np.nan)
        pval = model.pvalues.get("treatment_x_post", np.nan)
        ci = model.conf_int().loc["treatment_x_post"].tolist() if "treatment_x_post" in model.conf_int().index else [np.nan, np.nan]

        result = {
            "event": event,
            "event_date": event_date,
            "att": round(float(att), 4),
            "pvalue": round(float(pval), 6),
            "conf_int_lower": round(float(ci[0]), 4),
            "conf_int_upper": round(float(ci[1]), 4),
            "n_obs": len(df),
            "n_treated_stores": df[df["treatment"] == 1]["store_id"].nunique(),
            "n_control_stores": df[df["treatment"] == 0]["store_id"].nunique(),
        }
        logger.info("DiD ATT estimate: %.4f (p=%.4f)", att, pval)
        return result
    except Exception:
        logger.exception("DiD regression failed")
        return {}


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", default="bird_flu_2024")
    parser.add_argument("--event_date", default="2024-02-01")
    parser.add_argument("--category", default="Eggs")
    args = parser.parse_args()

    result = run_did_analysis(event=args.event, event_date=args.event_date, item_category=args.category)
    print("DiD result:", result)
