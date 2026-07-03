"""
OpenAQ API Exploration Script
------------------------------
Purpose: Understand the API structure before building the pipeline.
This script is NOT part of the pipeline — it is a discovery tool.
Run it manually: python3 explore_openaq.py
"""

import requests
import os
from pprint import pprint

API_KEY  = os.environ.get("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"
HEADERS  = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
}

def pretty_print(label, data):
    """Print data clearly with a label."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    pprint(data)

def explore_countries():
    """
    QUESTION: What countries does OpenAQ have data for?
    ENDPOINT: GET /v3/countries
    Returns a dict mapping ISO code -> numeric ID
    e.g. {"NG": 142, "US": 155, "GB": 63}
    We need the numeric ID for all subsequent queries.
    """
    print("\n🌍 EXPLORING: Countries")

    url    = f"{BASE_URL}/countries"
    params = {"limit": 200, "page": 1}   # fetch all countries in one call

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    countries = data.get("results", [])
    print(f"\n  Total countries available: {data['meta']['found']}")

    pretty_print("First country record (full structure)", countries[0] if countries else "EMPTY")

    # Build a lookup map: ISO code -> numeric ID
    # This is what we will use in every other function
    country_map = {}
    for c in countries:
        iso_code   = c.get("code")       # e.g. "NG"
        numeric_id = c.get("id")         # e.g. 142
        name       = c.get("name")       # e.g. "Nigeria"
        if iso_code and numeric_id:
            country_map[iso_code] = {
                "id":   numeric_id,
                "name": name
            }

    # Show a sample of the map so we can confirm it looks right
    sample_countries = ["NG", "US", "GB", "DE", "IN", "BR", "ZA"]
    print("\n  Numeric IDs for our target countries:")
    for iso in sample_countries:
        entry = country_map.get(iso)
        if entry:
            print(f"     {iso} ({entry['name']}): numeric ID = {entry['id']}")
        else:
            print(f"     {iso}: NOT FOUND in API")

    return country_map

def explore_locations(country_numeric_id, country_name="Nigeria"):
    """
    QUESTION: What monitoring stations exist for a country?
    ENDPOINT: GET /v3/locations
    NOTE: API requires numeric country ID, not ISO string code.
    """
    print(f"\n📍 EXPLORING: Locations in {country_name} (ID: {country_numeric_id})")

    url    = f"{BASE_URL}/locations"
    params = {
        "countries_id": country_numeric_id,   # numeric ID, not "NG"
        "limit": 3,
        "page":  1
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    locations = data.get("results", [])
    print(f"\n  Total locations in {country_name}: {data['meta']['found']}")

    if not locations:
        print(f"  ⚠️  No locations found for {country_name}")
        return []

    pretty_print("First location record (full structure)", locations[0])

    # Extract and display sensors clearly
    # Each location has multiple sensors (one per pollutant)
    # The sensor ID is what we need to fetch actual measurements
    print(f"\n  📡 Sensors in first location ({locations[0].get('name')}):")
    for sensor in locations[0].get("sensors", []):
        print(f"     Sensor ID : {sensor['id']}")
        print(f"     Parameter : {sensor['parameter']['name']}")
        print(f"     Units     : {sensor['parameter']['units']}")
        print()

    return locations

def explore_measurements(sensor_id):
    """
    QUESTION: What does actual measurement data look like?
    ENDPOINT: GET /v3/sensors/{sensor_id}/days
    Returns daily averages — the core of our pipeline.
    """
    print(f"\n📊 EXPLORING: Daily measurements for sensor {sensor_id}")

    url    = f"{BASE_URL}/sensors/{sensor_id}/days"
    params = {
        "date_from": "2024-01-01",
        "date_to":   "2024-01-07",
        "limit": 7
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()

    measurements = data.get("results", [])
    print(f"\n  Measurements returned: {len(measurements)}")

    if not measurements:
        print("  ⚠️  No measurements found for this sensor in date range")
        return []

    pretty_print("First measurement record (full structure)", measurements[0])

    # Print each field with its type and value
    # This directly tells us how to write our dbt staging model
    print("\n  📋 Fields in each measurement record:")
    for key, value in measurements[0].items():
        print(f"     {key}: {type(value).__name__} = {value}")

    return measurements

def check_data_quality(sensor_id):
    """
    QUESTION: Can we trust this data?
    Checks for nulls, negatives, and date gaps over 3 months.
    These findings directly inform our dbt staging filters.
    """
    print(f"\n🔍 DATA QUALITY CHECK for sensor {sensor_id}")

    url    = f"{BASE_URL}/sensors/{sensor_id}/days"
    params = {
        "date_from": "2024-01-01",
        "date_to":   "2024-03-31",
        "limit": 100
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    measurements = response.json().get("results", [])

    print(f"\n  Total records returned : {len(measurements)}")
    print(f"  Expected (daily)       : ~90 records")

    # Check nulls
    null_values = [m for m in measurements if m.get("value") is None]
    print(f"  Null values            : {len(null_values)}")

    # Check negatives
    negative_values = [
        m for m in measurements
        if m.get("value") is not None and m["value"] < 0
    ]
    print(f"  Negative values        : {len(negative_values)}")

    # Value range
    values = [m["value"] for m in measurements if m.get("value") is not None]
    if values:
        print(f"  Min value              : {min(values):.2f}")
        print(f"  Max value              : {max(values):.2f}")
        print(f"  Avg value              : {sum(values)/len(values):.2f}")

    # Date coverage — how to extract dates depends on what explore_measurements showed us
    try:
        dates = sorted([
            m["period"]["datetimeFrom"]["utc"][:10]
            for m in measurements
        ])
        if dates:
            print(f"\n  Date range             : {dates[0]}  →  {dates[-1]}")
            print(f"  Days with data         : {len(dates)}")
        else:
            print("\n  No dates found")
    except (KeyError, TypeError) as e:
        print(f"\n  ⚠️  Could not extract dates — structure may differ: {e}")
        print("       Check the measurement record structure above")


# ─────────────────────────────────────────────
# RUN THE EXPLORATION
# ─────────────────────────────────────────────
if __name__ == "__main__":

    print("🚀 Starting OpenAQ API Exploration")
    print("Discovering data structure before building the pipeline.\n")

    # Step 1: Get all countries and build ISO -> numeric ID map
    country_map = explore_countries()

    # Step 2: Use Nigeria's numeric ID to query locations
    nigeria = country_map.get("NG")
    if not nigeria:
        print("\n❌ Nigeria not found in country list. Check API response.")
        exit(1)

    locations = explore_locations(
        country_numeric_id = nigeria["id"],
        country_name       = nigeria["name"]
    )

    # Step 3: Use first available sensor to explore measurements
    if locations and locations[0].get("sensors"):
        first_sensor = locations[0]["sensors"][0]
        sensor_id    = first_sensor["id"]

        print(f"\n  Using sensor: {first_sensor['parameter']['name']} "
              f"(ID: {sensor_id}) from {locations[0].get('name')}")

        explore_measurements(sensor_id)
        check_data_quality(sensor_id)

    else:
        print("\n⚠️  No sensors found. Try a different country.")

    print("\n\n✅ Exploration complete!")
    print("Review the output above to understand the API structure.")
