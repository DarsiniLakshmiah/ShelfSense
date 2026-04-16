"""
Tests for ingestion/kroger_client.py

Uses 'responses' library to mock HTTP without real network calls.
"""

from __future__ import annotations

import json
import os
import time

import pytest
import responses as resp_lib

os.environ.setdefault("KROGER_CLIENT_ID", "test_id")
os.environ.setdefault("KROGER_CLIENT_SECRET", "test_secret")
os.environ.setdefault("KROGER_BASE_URL", "https://api.kroger.com/v1")

from ingestion.kroger_client import KrogerClient

TOKEN_RESPONSE = {
    "access_token": "mock_token_abc123",
    "token_type": "bearer",
    "expires_in": 1800,
}

PRODUCTS_RESPONSE = {
    "data": [
        {
            "productId": "0001111041700",
            "description": "Large Eggs Grade A",
            "upc": "0001111041700",
            "categories": ["Dairy & Eggs"],
            "items": [{"price": {"regular": 3.99, "promo": 2.99}, "size": "12 ct", "soldBy": "unit"}],
        }
    ],
    "meta": {"pagination": {"total": 1}},
}

LOCATIONS_RESPONSE = {
    "data": [
        {
            "locationId": "01400943",
            "name": "Kroger Test Store",
            "address": {"addressLine1": "123 Main St", "city": "Columbus", "state": "OH", "zipCode": "43201"},
        }
    ]
}


class TestKrogerClient:
    def setup_method(self) -> None:
        self.client = KrogerClient()

    @resp_lib.activate
    def test_get_access_token(self) -> None:
        resp_lib.add(
            resp_lib.POST,
            KrogerClient.TOKEN_URL,
            json=TOKEN_RESPONSE,
            status=200,
        )
        token = self.client.get_access_token()
        assert token == "mock_token_abc123"

    @resp_lib.activate
    def test_token_is_cached(self) -> None:
        resp_lib.add(
            resp_lib.POST,
            KrogerClient.TOKEN_URL,
            json=TOKEN_RESPONSE,
            status=200,
        )
        token1 = self.client.get_access_token()
        token2 = self.client.get_access_token()
        assert token1 == token2
        assert len(resp_lib.calls) == 1  # only one real HTTP call

    @resp_lib.activate
    def test_get_locations(self) -> None:
        resp_lib.add(resp_lib.POST, KrogerClient.TOKEN_URL, json=TOKEN_RESPONSE)
        resp_lib.add(
            resp_lib.GET,
            f"{KrogerClient.BASE_URL}/locations",
            json=LOCATIONS_RESPONSE,
        )
        locations = self.client.get_locations("43201", limit=1)
        assert len(locations) == 1
        assert locations[0]["locationId"] == "01400943"

    @resp_lib.activate
    def test_get_products(self) -> None:
        resp_lib.add(resp_lib.POST, KrogerClient.TOKEN_URL, json=TOKEN_RESPONSE)
        resp_lib.add(
            resp_lib.GET,
            f"{KrogerClient.BASE_URL}/products",
            json=PRODUCTS_RESPONSE,
        )
        products = self.client.get_products("eggs", "01400943", limit=5)
        assert len(products) == 1
        assert products[0]["description"] == "Large Eggs Grade A"

    @resp_lib.activate
    def test_get_products_retries_on_500(self) -> None:
        resp_lib.add(resp_lib.POST, KrogerClient.TOKEN_URL, json=TOKEN_RESPONSE)
        # First call fails, second succeeds
        resp_lib.add(resp_lib.GET, f"{KrogerClient.BASE_URL}/products", status=500)
        resp_lib.add(resp_lib.GET, f"{KrogerClient.BASE_URL}/products", json=PRODUCTS_RESPONSE)
        # tenacity will retry — this should not raise
        products = self.client.get_products("eggs", "01400943")
        assert isinstance(products, list)
