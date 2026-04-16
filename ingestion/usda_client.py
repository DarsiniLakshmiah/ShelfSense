from __future__ import annotations

"""
USDA NASS Quick Stats API client.

Returns weekly wholesale commodity prices (eggs, beef, chicken, produce).
These are the upstream supply signals used as features in causal models.

Docs: https://quickstats.nass.usda.gov/api
"""

import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)

USDA_BASE_URL = "https://quickstats.nass.usda.gov/api"


class USDAClient:
    """USDA NASS Quick Stats API client."""

    def __init__(self) -> None:
        self.api_key: str = os.environ["USDA_API_KEY"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
    def get_commodity_prices(
        self,
        commodity_desc: str,
        year_start: int,
        year_end: int,
        freq_desc: str = "WEEKLY",
        statisticcat_desc: str = "PRICE RECEIVED",
    ) -> list[dict[str, Any]]:
        """
        Fetch weekly USDA commodity prices.

        Args:
            commodity_desc: e.g. "EGGS", "BROILERS", "MILK"
            year_start: first year to pull (inclusive)
            year_end: last year to pull (inclusive)
            freq_desc: frequency — "WEEKLY", "MONTHLY", etc.
            statisticcat_desc: USDA statistic category

        Returns:
            List of price records as dicts.
        """
        logger.info(
            "Fetching USDA prices: commodity=%s years=%d-%d",
            commodity_desc,
            year_start,
            year_end,
        )
        params: dict[str, Any] = {
            "key": self.api_key,
            "commodity_desc": commodity_desc.upper(),
            "statisticcat_desc": statisticcat_desc,
            "freq_desc": freq_desc,
            "year__GE": year_start,
            "year__LE": year_end,
            "format": "JSON",
        }
        response = requests.get(f"{USDA_BASE_URL}/api_GET/", params=params, timeout=90)
        response.raise_for_status()
        data = response.json()
        records = data.get("data", [])
        logger.info("USDA returned %d record(s) for %s", len(records), commodity_desc)
        return records

    def get_egg_prices(self, year_start: int, year_end: int) -> list[dict[str, Any]]:
        return self.get_commodity_prices("EGGS", year_start, year_end)

    def get_beef_prices(self, year_start: int, year_end: int) -> list[dict[str, Any]]:
        return self.get_commodity_prices("CATTLE", year_start, year_end)

    def get_chicken_prices(self, year_start: int, year_end: int) -> list[dict[str, Any]]:
        return self.get_commodity_prices("BROILERS", year_start, year_end)

    def get_milk_prices(self, year_start: int, year_end: int) -> list[dict[str, Any]]:
        return self.get_commodity_prices("MILK", year_start, year_end)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = USDAClient()
    records = client.get_egg_prices(2024, 2025)
    print(f"Egg price records: {len(records)}")
    if records:
        print(records[0])
