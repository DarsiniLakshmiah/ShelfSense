"""
Tests for ML modules (forecaster and classifier).

All Snowflake I/O is mocked — no real DB connection needed.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test")
os.environ.setdefault("SNOWFLAKE_USER", "test")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "test")


def _make_price_df(n: int = 120) -> pd.DataFrame:
    """Synthetic mart_price_history DataFrame for ML tests."""
    dates = pd.date_range("2024-09-01", periods=n, freq="D")
    prices = 3.99 + np.sin(np.linspace(0, 4 * np.pi, n)) * 0.5 + np.random.normal(0, 0.05, n)
    return pd.DataFrame(
        {
            "ITEM_ID": "EGG001",
            "STORE_ID": "STORE01",
            "PRICE_DATE": dates,
            "REGULAR_PRICE": prices.round(2),
            "PRICE_DELTA_7D": pd.Series(prices).diff(7).values.round(4),
            "PRICE_DELTA_30D": pd.Series(prices).diff(30).values.round(4),
            "PRICE_VS_90D_AVG": (pd.Series(prices) - pd.Series(prices).rolling(90, min_periods=1).mean()).values.round(4),
            "IS_ON_SALE": [i % 7 == 0 for i in range(n)],
            "CATEGORY": "Eggs",
        }
    )


class TestBuyNowClassifier:
    @patch("ml.classifier._load_training_data")
    def test_train_returns_metrics(self, mock_load: MagicMock) -> None:
        mock_load.return_value = _make_price_df(120)

        from ml.classifier import BuyNowClassifier

        clf = BuyNowClassifier()
        metrics = clf.train(lookback_days=90)

        assert isinstance(metrics, dict)
        assert "f1" in metrics
        assert "auc_roc" in metrics
        assert 0.0 <= metrics["f1"] <= 1.0

    @patch("ml.classifier._load_training_data")
    def test_train_with_insufficient_data(self, mock_load: MagicMock) -> None:
        mock_load.return_value = pd.DataFrame()

        from ml.classifier import BuyNowClassifier

        clf = BuyNowClassifier()
        metrics = clf.train(lookback_days=90)
        assert metrics == {}

    @patch("ml.classifier._load_training_data")
    def test_predict_proba_shape(self, mock_load: MagicMock) -> None:
        mock_load.return_value = _make_price_df(120)

        from ml.classifier import BuyNowClassifier

        clf = BuyNowClassifier()
        clf.train(lookback_days=90)

        # Build a small feature matrix
        X = pd.DataFrame(
            {
                "price_delta_7d": [0.1, -0.2],
                "price_delta_30d": [0.3, -0.5],
                "price_vs_90d_avg": [0.0, 0.1],
                "is_on_sale": [0, 1],
                "day_of_week": [1, 3],
                "week_of_month": [2, 4],
                "month": [3, 3],
            }
        )
        proba = clf.predict_proba(X)
        assert proba.shape == (2,)
        assert all(0.0 <= p <= 1.0 for p in proba)

    @patch("ml.classifier._load_training_data")
    def test_recommendation_threshold(self, mock_load: MagicMock) -> None:
        from ml.classifier import BUY_NOW_THRESHOLD

        assert 0 < BUY_NOW_THRESHOLD <= 1.0
        assert BUY_NOW_THRESHOLD == 0.6


class TestPriceForecaster:
    @patch("ml.forecaster._load_price_history")
    def test_train_returns_metrics(self, mock_load: MagicMock) -> None:
        mock_load.return_value = _make_price_df(60)

        from ml.forecaster import PriceForecaster

        f = PriceForecaster()
        metrics = f.train(lookback_days=60)
        assert isinstance(metrics, dict)
        assert "n_models" in metrics
        assert metrics["n_models"] >= 0

    @patch("ml.forecaster._load_price_history")
    def test_train_with_empty_data(self, mock_load: MagicMock) -> None:
        mock_load.return_value = pd.DataFrame()

        from ml.forecaster import PriceForecaster

        f = PriceForecaster()
        metrics = f.train(lookback_days=90)
        assert metrics == {}
