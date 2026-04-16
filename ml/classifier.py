"""
Buy-Now Classifier: XGBoost binary classifier.

Target:  did price drop ≥10% within next 7 days? (1 = yes, 0 = no)
Threshold: 0.6 probability → "BUY_NOW" recommendation

Usage:
    clf = BuyNowClassifier()
    metrics = clf.train(lookback_days=90)
    clf.run_inference_and_write()
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BUY_NOW_THRESHOLD = 0.6
PRICE_DROP_PCT = 0.10  # 10% drop = positive label
MODEL_VERSION = "1.0.0"

FEATURE_COLS = [
    "price_delta_7d",
    "price_delta_30d",
    "price_vs_90d_avg",
    "is_on_sale",
    "day_of_week",
    "week_of_month",
    "month",
]


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


def _load_training_data(lookback_days: int) -> pd.DataFrame:
    conn = _get_conn()
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    sql = f"""
        SELECT
            item_id, store_id, price_date, regular_price,
            price_delta_7d, price_delta_30d, price_vs_90d_avg,
            is_on_sale
        FROM MARTS.MART_PRICE_HISTORY
        WHERE price_date >= '{cutoff}'
        ORDER BY item_id, store_id, price_date
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        conn.close()


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Overwrite PRICE_DATE in-place so that after column lowercasing there is
    # only one 'price_date' column (no duplicate that would make row[col] return
    # a Series and break Snowflake parameter binding).
    df["PRICE_DATE"] = pd.to_datetime(df["PRICE_DATE"])
    df["day_of_week"] = df["PRICE_DATE"].dt.dayofweek
    df["week_of_month"] = (df["PRICE_DATE"].dt.day - 1) // 7 + 1
    df["month"] = df["PRICE_DATE"].dt.month
    return df


def _build_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Label: 1 if price drops ≥10% in the next 7 days."""
    df = df.sort_values(["ITEM_ID", "STORE_ID", "PRICE_DATE"])
    df["future_price_7d"] = df.groupby(["ITEM_ID", "STORE_ID"])["REGULAR_PRICE"].shift(-7)
    df["label"] = (
        (df["future_price_7d"] - df["REGULAR_PRICE"]) / df["REGULAR_PRICE"].replace(0, np.nan)
        <= -PRICE_DROP_PCT
    ).astype(int)
    # Drop rows where we can't compute the label (last 7 rows per group)
    return df.dropna(subset=["future_price_7d", "label"])


class BuyNowClassifier:
    """XGBoost buy-now vs. wait binary classifier."""

    def __init__(self) -> None:
        self.model: Any = None
        self._feature_cols: list[str] = FEATURE_COLS

    def train(self, lookback_days: int = 90) -> dict[str, float]:
        """Train XGBoost on historical price data. Returns evaluation metrics."""
        import xgboost as xgb
        from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
        from sklearn.model_selection import train_test_split

        df = _load_training_data(lookback_days)
        if df.empty:
            logger.warning("No training data — skipping classifier training")
            return {}

        df = _engineer_features(df)
        df = _build_labels(df)

        # Normalize column names to lowercase for consistent feature matrix
        df.columns = [c.lower() for c in df.columns]
        available = [c for c in self._feature_cols if c in df.columns]

        X = df[available].fillna(0).astype(float)
        y = df["label"].values

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

        self.model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            scale_pos_weight=(y_train == 0).sum() / max((y_train == 1).sum(), 1),
            eval_metric="logloss",
            verbosity=0,
        )
        self.model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = self.model.predict(X_test)
        y_prob = self.model.predict_proba(X_test)[:, 1]

        metrics = {
            "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
            "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
            "f1": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
            "auc_roc": round(float(roc_auc_score(y_test, y_prob)) if len(np.unique(y_test)) > 1 else 0.0, 4),
        }
        logger.info("Classifier training complete: %s", metrics)
        return metrics

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return buy-now probability scores."""
        if self.model is None:
            raise RuntimeError("Model not trained. Call .train() first.")
        return self.model.predict_proba(X.fillna(0).astype(float))[:, 1]

    def run_inference_and_write(self) -> None:
        """
        Run inference on latest mart_price_history and write results
        to MARTS.MART_BUY_SIGNALS via Snowflake.
        """
        try:
            from ml.forecaster import PriceForecaster
        except ModuleNotFoundError:
            from forecaster import PriceForecaster

        if self.model is None:
            logger.info("Model not loaded — training now with 90-day lookback")
            self.train(lookback_days=90)

        logger.info("Training price forecaster for 7d/14d predictions")
        forecaster = PriceForecaster()
        forecaster.train(lookback_days=90)

        conn = _get_conn()
        try:
            cur = conn.cursor()
            # Fetch latest snapshot
            cur.execute("""
                SELECT item_id, store_id, price_date, regular_price,
                       price_delta_7d, price_delta_30d, price_vs_90d_avg, is_on_sale
                FROM MARTS.MART_PRICE_HISTORY
                WHERE price_date = (SELECT MAX(price_date) FROM MARTS.MART_PRICE_HISTORY)
            """)
            df = cur.fetch_pandas_all()

            if df.empty:
                logger.warning("No rows in mart_price_history to score")
                return

            df = _engineer_features(df)
            df.columns = [c.lower() for c in df.columns]

            available = [c for c in self._feature_cols if c in df.columns]
            X = df[available].fillna(0).astype(float)

            probas = self.predict_proba(X)
            df["buy_now_probability"] = probas
            df["recommendation"] = df["buy_now_probability"].apply(
                lambda p: "BUY_NOW" if p >= BUY_NOW_THRESHOLD else ("WAIT" if p < 0.4 else "NEUTRAL")
            )

            # Build forecast map — one predict() call per unique item × store combo
            forecast_map: dict[tuple, tuple] = {}
            combos = df[["item_id", "store_id"]].drop_duplicates()
            for row in combos.itertuples(index=False):
                try:
                    fc = forecaster.predict(row.item_id, row.store_id, horizon_days=14)
                    forecast_map[(row.item_id, row.store_id)] = (
                        round(float(fc["yhat"].iloc[6]), 4),   # day 7
                        round(float(fc["yhat"].iloc[13]), 4),  # day 14
                    )
                except (ValueError, IndexError):
                    forecast_map[(row.item_id, row.store_id)] = (None, None)

            df["predicted_price_7d"] = df.apply(
                lambda r: forecast_map.get((r["item_id"], r["store_id"]), (None, None))[0], axis=1
            )
            df["predicted_price_14d"] = df.apply(
                lambda r: forecast_map.get((r["item_id"], r["store_id"]), (None, None))[1], axis=1
            )
            logger.info("Forecasts generated for %d item×store combos", len(forecast_map))

            # Remove today's existing signals before rewriting to avoid duplicates
            cur.execute("""
                DELETE FROM MARTS.MART_BUY_SIGNALS
                WHERE signal_date = (SELECT MAX(price_date) FROM MARTS.MART_PRICE_HISTORY)
            """)
            conn.commit()

            # Bulk-load into MART_BUY_SIGNALS using write_pandas (single round-trip).
            from snowflake.connector.pandas_tools import write_pandas

            out_df = pd.DataFrame({
                "ITEM_ID": df["item_id"],
                "STORE_ID": df["store_id"],
                "SIGNAL_DATE": df["price_date"].apply(
                    lambda x: str(x.date()) if hasattr(x, "date") else str(x)
                ),
                "REGULAR_PRICE": df["regular_price"].astype(float),
                "PREDICTED_PRICE_7D": df["predicted_price_7d"],
                "PREDICTED_PRICE_14D": df["predicted_price_14d"],
                "BUY_NOW_PROBABILITY": df["buy_now_probability"].astype(float),
                "RECOMMENDATION": df["recommendation"],
                "MODEL_VERSION": MODEL_VERSION,
            })

            _, _, nrows, _ = write_pandas(
                conn, out_df, "MART_BUY_SIGNALS", schema="MARTS", auto_create_table=False
            )
            logger.info("Wrote %d buy signal rows to MARTS.MART_BUY_SIGNALS", nrows)
        finally:
            conn.close()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    clf = BuyNowClassifier()
    if args.backtest:
        metrics = clf.train(lookback_days=args.days)
        print("Backtest metrics:", metrics)
    else:
        clf.run_inference_and_write()
