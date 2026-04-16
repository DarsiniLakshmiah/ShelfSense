"""
Tests for dbt model logic (SQL transformations).

Because we can't run dbt in unit tests, we replicate the SQL logic in
pandas to verify the transformation rules are correct.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _make_price_history() -> pd.DataFrame:
    """Create a minimal price history DataFrame matching stg_kroger_prices output."""
    dates = pd.date_range("2025-01-01", periods=100, freq="D")
    return pd.DataFrame(
        {
            "item_id": "EGG001",
            "store_id": "STORE01",
            "price_date": dates,
            "regular_price": [3.99 + 0.01 * i % 5 for i in range(100)],
            "promo_price": [None if i % 7 != 0 else 2.99 for i in range(100)],
            "is_on_sale": [i % 7 == 0 for i in range(100)],
        }
    )


class TestPriceHistoryTransformation:
    def test_price_delta_7d_calculation(self) -> None:
        df = _make_price_history()
        df = df.sort_values("price_date")
        df["price_delta_7d"] = df["regular_price"] - df["regular_price"].shift(7)
        # First 7 rows should be NaN
        assert df["price_delta_7d"].iloc[:7].isna().all()
        # Row 7 should be price[7] - price[0]
        expected = df["regular_price"].iloc[7] - df["regular_price"].iloc[0]
        assert abs(df["price_delta_7d"].iloc[7] - expected) < 1e-9

    def test_price_delta_30d_calculation(self) -> None:
        df = _make_price_history()
        df = df.sort_values("price_date")
        df["price_delta_30d"] = df["regular_price"] - df["regular_price"].shift(30)
        assert df["price_delta_30d"].iloc[:30].isna().all()

    def test_price_vs_90d_avg(self) -> None:
        df = _make_price_history()
        df = df.sort_values("price_date")
        df["rolling_90d"] = df["regular_price"].shift(1).rolling(90, min_periods=1).mean()
        df["price_vs_90d_avg"] = df["regular_price"] - df["rolling_90d"]
        # First row: rolling mean of nothing before it → vs 0 essentially → ~3.99
        assert not df["price_vs_90d_avg"].isna().all()

    def test_no_negative_prices(self) -> None:
        df = _make_price_history()
        assert (df["regular_price"] >= 0).all()

    def test_is_on_sale_matches_promo(self) -> None:
        df = _make_price_history()
        # Items with promo_price should have is_on_sale == True
        has_promo = df["promo_price"].notna()
        assert (df.loc[has_promo, "is_on_sale"] == True).all()


class TestPromoCalendarLogic:
    def test_sale_start_detection(self) -> None:
        """A sale starts when is_on_sale transitions False → True."""
        df = pd.DataFrame(
            {
                "price_date": pd.date_range("2025-01-01", periods=10),
                "is_on_sale": [False, False, True, True, False, False, True, True, False, False],
            }
        )
        df["prev_on_sale"] = df["is_on_sale"].shift(1)
        sale_starts = df[(df["is_on_sale"]) & (df["prev_on_sale"] == False)]
        assert len(sale_starts) == 2
        assert sale_starts.iloc[0]["price_date"] == pd.Timestamp("2025-01-03")

    def test_sale_depth_pct(self) -> None:
        regular = 4.00
        promo = 2.99
        depth = round(1.0 - promo / regular, 4)
        assert depth == pytest.approx(0.2525, abs=1e-4)
