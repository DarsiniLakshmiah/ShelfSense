from __future__ import annotations

"""
Kroger API client — OAuth 2.0 client credentials + Products/Locations endpoints.

Usage:
    client = KrogerClient()
    token  = client.get_access_token()
    stores = client.get_locations(zip_code="10001")
    items  = client.get_products(term="eggs", location_id=stores[0]["locationId"])
"""

import logging
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)


class KrogerClient:
    """Client for the Kroger Developer API (OAuth 2.0 client credentials)."""

    BASE_URL: str = os.environ.get("KROGER_BASE_URL", "https://api.kroger.com/v1")
    TOKEN_URL: str = "https://api.kroger.com/v1/connect/oauth2/token"

    def __init__(self) -> None:
        self.client_id: str = os.environ["KROGER_CLIENT_ID"]
        self.client_secret: str = os.environ["KROGER_CLIENT_SECRET"]
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def get_access_token(self) -> str:
        """Return a valid OAuth access token, refreshing if expired."""
        if self._access_token and time.time() < self._token_expires_at - 30:
            return self._access_token

        logger.info("Fetching new Kroger OAuth token")
        response = requests.post(
            self.TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "scope": "product.compact",
            },
            auth=(self.client_id, self.client_secret),
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        self._token_expires_at = time.time() + payload.get("expires_in", 1800)
        logger.info("Kroger OAuth token obtained (expires in %ds)", payload.get("expires_in"))
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_locations(self, zip_code: str, limit: int = 5) -> list[dict[str, Any]]:
        """Return nearby Kroger store locations for a ZIP code."""
        logger.info("Fetching Kroger locations for zip=%s", zip_code)
        response = requests.get(
            f"{self.BASE_URL}/locations",
            headers=self._auth_headers(),
            params={"filter.zipCode": zip_code, "filter.limit": limit},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        locations = data.get("data", [])
        logger.info("Found %d location(s) for zip=%s", len(locations), zip_code)
        return locations

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_products(
        self,
        term: str,
        location_id: str,
        limit: int = 50,
        start: int = 1,
    ) -> list[dict[str, Any]]:
        """Search for products by term at a specific store location."""
        logger.info("Fetching products term='%s' location=%s", term, location_id)
        response = requests.get(
            f"{self.BASE_URL}/products",
            headers=self._auth_headers(),
            params={
                "filter.term": term,
                "filter.locationId": location_id,
                "filter.limit": limit,
                "filter.start": start,
            },
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        products = data.get("data", [])
        logger.info("Got %d product(s)", len(products))
        return products

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_product(self, product_id: str, location_id: str) -> dict[str, Any]:
        """Fetch a single product by ID with pricing for a given location."""
        logger.info("Fetching product %s at location %s", product_id, location_id)
        response = requests.get(
            f"{self.BASE_URL}/products/{product_id}",
            headers=self._auth_headers(),
            params={"filter.locationId": location_id},
            timeout=15,
        )
        response.raise_for_status()
        return response.json().get("data", {})

    # ------------------------------------------------------------------
    # Bulk helper
    # ------------------------------------------------------------------

    def get_all_tracked_products(
        self,
        search_terms: list[str],
        location_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Fetch products for all (term, location) combinations."""
        results: list[dict[str, Any]] = []
        for location_id in location_ids:
            for term in search_terms:
                try:
                    products = self.get_products(term=term, location_id=location_id)
                    for product in products:
                        product["_location_id"] = location_id
                        product["_search_term"] = term
                    results.extend(products)
                except Exception:
                    logger.exception("Failed to fetch term='%s' location=%s", term, location_id)
        return results


# ------------------------------------------------------------------
# Quick smoke-test when run directly
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = KrogerClient()
    token = client.get_access_token()
    print(f"Token obtained: {token[:20]}...")
    locations = client.get_locations("10001", limit=2)
    if locations:
        loc_id = locations[0]["locationId"]
        print(f"Store: {locations[0].get('name')} ({loc_id})")
        products = client.get_products("eggs", loc_id, limit=5)
        print(f"Products found: {len(products)}")
        for p in products[:3]:
            print(f"  {p.get('description')} — {p.get('items', [{}])[0].get('price', {})}")
