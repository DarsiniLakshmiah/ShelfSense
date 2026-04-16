from __future__ import annotations

"""
NOAA Climate Data Online (CDO) API client.

Used to pull frost events, heat waves, and drought signals as
supply shock indicators for the causal inference models.

Token: https://www.ncdc.noaa.gov/cdo-web/token
Docs:  https://www.ncdc.noaa.gov/cdo-web/webservices/v2
"""

import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)

NOAA_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"


class NOAAClient:
    """NOAA CDO REST API client."""

    def __init__(self) -> None:
        self.token: str = os.environ["NOAA_TOKEN"]

    def _headers(self) -> dict[str, str]:
        return {"token": self.token}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
    def get_data(
        self,
        dataset_id: str,
        start_date: str,
        end_date: str,
        data_type_ids: list[str] | None = None,
        location_id: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Fetch climate data from NOAA CDO.

        Args:
            dataset_id: e.g. "GHCND" (daily summaries)
            start_date: ISO date string "YYYY-MM-DD"
            end_date:   ISO date string "YYYY-MM-DD"
            data_type_ids: e.g. ["TMAX","TMIN","PRCP","SNOW"]
            location_id: e.g. "FIPS:19" (Iowa), "FIPS:06" (California)
            limit: max records per request (max 1000)

        Returns:
            List of climate observation records.
        """
        logger.info(
            "Fetching NOAA data: dataset=%s %s→%s location=%s",
            dataset_id,
            start_date,
            end_date,
            location_id,
        )
        params: dict[str, Any] = {
            "datasetid": dataset_id,
            "startdate": start_date,
            "enddate": end_date,
            "limit": limit,
            "units": "standard",
        }
        if data_type_ids:
            params["datatypeid"] = ",".join(data_type_ids)
        if location_id:
            params["locationid"] = location_id

        response = requests.get(
            f"{NOAA_BASE_URL}/data",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        records = data.get("results", [])
        logger.info("NOAA returned %d record(s)", len(records))
        return records

    def get_weather_anomalies(
        self,
        start_date: str,
        end_date: str,
        location_id: str = "FIPS:06",  # California (large produce state)
    ) -> list[dict[str, Any]]:
        """Pull TMAX/TMIN/PRCP/SNOW for anomaly detection."""
        return self.get_data(
            dataset_id="GHCND",
            start_date=start_date,
            end_date=end_date,
            data_type_ids=["TMAX", "TMIN", "PRCP", "SNOW"],
            location_id=location_id,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = NOAAClient()
    records = client.get_weather_anomalies("2025-01-01", "2025-01-07")
    print(f"NOAA records: {len(records)}")
    if records:
        print(records[0])
