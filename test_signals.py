from dotenv import load_dotenv
load_dotenv()
import os, requests, logging
logging.basicConfig(level=logging.INFO)

print("--- Testing BLS ---")
try:
    api_key = os.environ.get("BLS_API_KEY", "NOT_SET")
    print(f"BLS_API_KEY: {'SET' if api_key != 'NOT_SET' else 'MISSING'}")
    resp = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json={"seriesid": ["APU0000708111"], "startyear": "2024", "endyear": "2026",
              "registrationkey": api_key},
        timeout=30,
    )
    print(f"HTTP status: {resp.status_code}")
    data = resp.json()
    print(f"Status: {data.get('status')}")
    series = data.get("Results", {}).get("series", [])
    if series:
        print(f"Data points: {len(series[0].get('data', []))}")
        print(f"Sample: {series[0]['data'][0]}")
    else:
        print("No series data returned")
except Exception as e:
    print(f"BLS ERROR: {e}")

print("\n--- Testing NOAA ---")
try:
    token = os.environ.get("NOAA_TOKEN", "NOT_SET")
    print(f"NOAA_TOKEN: {'SET' if token != 'NOT_SET' else 'MISSING'}")
    resp = requests.get(
        "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
        headers={"token": token},
        params={"datasetid": "GHCND", "startdate": "2026-04-01", "enddate": "2026-04-07",
                "datatypeid": "TMAX", "locationid": "FIPS:06", "limit": 5},
        timeout=30,
    )
    print(f"HTTP status: {resp.status_code}")
    print(f"Response: {resp.text[:300]}")
except Exception as e:
    print(f"NOAA ERROR: {e}")

print("\n--- Testing USDA (60s timeout) ---")
try:
    api_key = os.environ.get("USDA_API_KEY", "NOT_SET")
    print(f"USDA_API_KEY: {'SET' if api_key != 'NOT_SET' else 'MISSING'}")
    resp = requests.get(
        "https://quickstats.nass.usda.gov/api/api_GET/",
        params={"key": api_key, "commodity_desc": "EGGS",
                "statisticcat_desc": "PRICE RECEIVED",
                "freq_desc": "WEEKLY", "year__GE": 2026, "format": "JSON"},
        timeout=60,
    )
    print(f"HTTP status: {resp.status_code}")
    print(f"Response: {resp.text[:300]}")
except Exception as e:
    print(f"USDA ERROR: {e}")
