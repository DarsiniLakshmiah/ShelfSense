"""
Price Forecaster: Prophet (seasonality) + XGBoost (residuals + exogenous features).

Target:  regular_price at item × store level for next 14 days.
Usage:
    forecaster = PriceForecaster()
    metrics = forecaster.train(lookback_days=90)
    forecasts = forecaster.predict(item_id="0001111041700", store_id="01400943")
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


def _load_price_history(lookback_days: int = 90) -> pd.DataFrame:
    """Load mart_price_history from Snowflake for the past N days."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        schema="MARTS",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    sql = f"""
        SELECT
            item_id, store_id, price_date, regular_price,
            price_delta_7d, price_delta_30d, price_vs_90d_avg,
            is_on_sale, category
        FROM MARTS.MART_PRICE_HISTORY
        WHERE price_date >= '{cutoff}'
        ORDER BY item_id, store_id, price_date
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        logger.info("Loaded %d price history rows (lookback=%dd)", len(df), lookback_days)
        return df
    finally:
        conn.close()


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add time-based and lag features used by XGBoost."""
    df = df.copy()
    df["price_date"] = pd.to_datetime(df["PRICE_DATE"])
    df["day_of_week"] = df["price_date"].dt.dayofweek
    df["week_of_month"] = (df["price_date"].dt.day - 1) // 7 + 1
    df["month"] = df["price_date"].dt.month
    df["year"] = df["price_date"].dt.year
    return df


class PriceForecaster:
    """Prophet + XGBoost two-stage price forecaster."""

    MODEL_VERSION = "1.0.0"

    def __init__(self) -> None:
        self.prophet_models: dict[str, Any] = {}  # keyed by "item_id|store_id"
        self.xgb_models: dict[str, Any] = {}

    def train(self, lookback_days: int = 90) -> dict[str, float]:
        """
        Train Prophet + XGBoost models for all item × store combos.

        Returns aggregated MAE / MAPE metrics dict.
        """
        try:
            from prophet import Prophet
        except ImportError:
            logger.error("Prophet not installed. Run: pip install prophet")
            raise

        try:
            import xgboost as xgb
        except ImportError:
            logger.error("XGBoost not installed. Run: pip install xgboost")
            raise

        df = _load_price_history(lookback_days)
        if df.empty:
            logger.warning("No price history data available — skipping training")
            return {}

        df = _engineer_features(df)
        groups = df.groupby(["ITEM_ID", "STORE_ID"])

        all_mae, all_mape = [], []

        for (item_id, store_id), group in groups:
            key = f"{item_id}|{store_id}"
            group = group.sort_values("price_date")

            if len(group) < 14:
                continue

            # --- Prophet stage ---
            prophet_df = group[["price_date", "REGULAR_PRICE"]].rename(
                columns={"price_date": "ds", "REGULAR_PRICE": "y"}
            )
            prophet_df["ds"] = pd.to_datetime(prophet_df["ds"])

            m = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                interval_width=0.95,
                uncertainty_samples=0,  # disables posterior sampling — avoids shape mismatch on small groups
            )
            # Train/test split: hold out last 14 days
            train_df = prophet_df.iloc[:-14]
            test_df = prophet_df.iloc[-14:]

            try:
                m.fit(train_df)
                future = m.make_future_dataframe(periods=14)
                forecast = m.predict(future)
            except Exception as exc:
                logger.warning("Prophet failed for %s — skipping: %s", key, exc)
                continue

            # Residuals on test set
            test_forecast = forecast.set_index("ds").loc[test_df["ds"].values, "yhat"]
            residuals = test_df.set_index("ds")["y"] - test_forecast

            mae = residuals.abs().mean()
            mape = (residuals.abs() / test_df.set_index("ds")["y"].replace(0, np.nan)).mean()
            all_mae.append(mae)
            all_mape.append(mape)

            self.prophet_models[key] = m
            logger.debug("Trained Prophet for %s — MAE=%.4f", key, mae)

            # --- XGBoost residual correction (placeholder features) ---
            feature_cols = ["day_of_week", "week_of_month", "month"]
            available = [c for c in feature_cols if c in group.columns]
            if available and len(group) >= 28:
                X = group[available].values
                y_res = group["REGULAR_PRICE"].values - group["REGULAR_PRICE"].shift(1).bfill().values
                model = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0)
                model.fit(X[:-14], y_res[:-14])
                self.xgb_models[key] = model

        metrics = {
            "mae_mean": float(np.mean(all_mae)) if all_mae else 0.0,
            "mape_mean": float(np.mean(all_mape)) if all_mape else 0.0,
            "n_models": len(self.prophet_models),
        }
        logger.info("Forecaster training complete: %s", metrics)
        return metrics

    def predict(
        self,
        item_id: str,
        store_id: str,
        horizon_days: int = 14,
    ) -> pd.DataFrame:
        """
        Return a DataFrame with predicted prices for the next horizon_days.

        Columns: ds (date), yhat, yhat_lower, yhat_upper
        """
        key = f"{item_id}|{store_id}"
        if key not in self.prophet_models:
            raise ValueError(f"No trained model for {key}. Run .train() first.")

        m = self.prophet_models[key]
        future = m.make_future_dataframe(periods=horizon_days)
        forecast = m.predict(future)
        available_cols = [c for c in ["ds", "yhat", "yhat_lower", "yhat_upper"] if c in forecast.columns]
        forecast = forecast[available_cols].tail(horizon_days)
        return forecast.reset_index(drop=True)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="ShelfSense Price Forecaster")
    parser.add_argument("--item", required=True, help="Item search term (e.g. 'eggs')")
    parser.add_argument("--store_id", required=True, help="Kroger store location ID")
    parser.add_argument("--backtest_days", type=int, default=90)
    args = parser.parse_args()

    f = PriceForecaster()
    metrics = f.train(lookback_days=args.backtest_days)
    print("Training metrics:", metrics)
