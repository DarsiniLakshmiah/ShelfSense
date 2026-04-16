from __future__ import annotations

"""
Open Food Facts Open Prices API client (no API key required).

Used to bootstrap historical price baselines across stores before
the Kroger pipeline has enough history.

API Docs: https://prices.openfoodfacts.org/api/docs
"""

import logging
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

OPEN_PRICES_BASE_URL = "https://prices.openfoodfacts.org/api/v1"


class OpenPricesClient:
    """Open Food Facts Open Prices API (unauthenticated)."""

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=15))
    def get_prices(
        self,
        product_code: str | None = None,
        category: str | None = None,
        country: str = "us",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """
        Fetch crowdsourced prices from Open Prices.

        Args:
            product_code: EAN/UPC barcode (optional)
            category: Open Food Facts category slug (optional)
            country: ISO country code (default "us")
            page: pagination page
            page_size: records per page (max 100)

        Returns:
            Full paginated response dict with 'items' and 'total' keys.
        """
        logger.info(
            "Fetching Open Prices: product=%s category=%s country=%s page=%d",
            product_code,
            category,
            country,
            page,
        )
        params: dict[str, Any] = {
            "country": country,
            "page": page,
            "size": page_size,
        }
        if product_code:
            params["product_code"] = product_code
        if category:
            params["category"] = category

        response = requests.get(
            f"{OPEN_PRICES_BASE_URL}/prices",
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        logger.info(
            "Open Prices returned %d/%d item(s)",
            len(data.get("items", [])),
            data.get("total", 0),
        )
        return data

    def get_all_pages(
        self,
        product_code: str | None = None,
        category: str | None = None,
        country: str = "us",
        max_pages: int = 10,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Paginate through all results up to max_pages."""
        all_items: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            data = self.get_prices(
                product_code=product_code,
                category=category,
                country=country,
                page=page,
                page_size=page_size,
            )
            items = data.get("items", [])
            all_items.extend(items)
            if len(items) < page_size:
                break  # last page
        return all_items


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = OpenPricesClient()
    data = client.get_prices(category="en:eggs", country="us", page_size=5)
    print(f"Total egg prices: {data.get('total')}")
    items = data.get("items", [])
    if items:
        print(items[0])
