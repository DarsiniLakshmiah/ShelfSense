from __future__ import annotations

"""
Bureau of Labor Statistics (BLS) Public Data API v2 client.

Key series:
  APU0000708111  — eggs, grade A large, per dozen (avg price)
  APU0000703112  — ground beef, per pound
  CUUR0000SAF11  — CPI food at home (all urban consumers)
  WPU01130105    — PPI eggs

Docs: https://www.bls.gov/developers/api_signature_v2.htm
"""

import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)

BLS_BASE_URL = "https://api.bls.gov/publicAPI/v2"

# Default series to track
DEFAULT_SERIES: list[str] = [
    "APU0000708111",  # eggs grade A large
    "APU0000703112",  # ground beef per lb
    "CUUR0000SAF11",  # CPI food at home
    "WPU01130105",    # PPI eggs
]


class BLSClient:
    """BLS Public Data API v2 client."""

    def __init__(self) -> None:
        self.api_key: str = os.environ["BLS_API_KEY"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
    def get_series(
        self,
        series_ids: list[str],
        start_year: str,
        end_year: str,
    ) -> list[dict[str, Any]]:
        """
        Fetch time-series data for a list of BLS series IDs.

        Args:
            series_ids: list of BLS series IDs (max 50 per request with key)
            start_year: e.g. "2023"
            end_year: e.g. "2025"

        Returns:
            List of series objects with 'seriesID' and 'data' keys.
        """
        logger.info(
            "Fetching BLS series: %s years=%s-%s",
            series_ids,
            start_year,
            end_year,
        )
        payload: dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": start_year,
            "endyear": end_year,
            "registrationkey": self.api_key,
            "catalog": False,
            "calculations": True,
            "annualaverage": False,
        }
        response = requests.post(
            f"{BLS_BASE_URL}/timeseries/data/",
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "REQUEST_SUCCEEDED":
            logger.warning("BLS API non-success status: %s — %s", result.get("status"), result.get("message"))
        series_list = result.get("Results", {}).get("series", [])
        logger.info("BLS returned %d series", len(series_list))
        return series_list

    def get_default_series(self, start_year: str, end_year: str) -> list[dict[str, Any]]:
        return self.get_series(DEFAULT_SERIES, start_year, end_year)

    def flatten_series(self, series_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Flatten nested BLS response into a list of flat records."""
        records: list[dict[str, Any]] = []
        for series in series_list:
            series_id = series.get("seriesID", "")
            for obs in series.get("data", []):
                records.append(
                    {
                        "series_id": series_id,
                        "year": obs.get("year"),
                        "period": obs.get("period"),
                        "period_name": obs.get("periodName"),
                        "value": obs.get("value"),
                        "footnotes": obs.get("footnotes"),
                    }
                )
        return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = BLSClient()
    series = client.get_default_series("2024", "2025")
    flat = client.flatten_series(series)
    print(f"Flat records: {len(flat)}")
    if flat:
        print(flat[0])
