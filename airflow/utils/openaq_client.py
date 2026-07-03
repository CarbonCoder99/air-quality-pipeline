"""
OpenAQ API Client
------------------
Reusable functions for interacting with the OpenAQ v3 API.
Used by the ingestion DAG — not a DAG itself.

Key lessons from exploration:
  - API requires NUMERIC country IDs, not ISO string codes
  - Many sensors are inactive (datetimeFirst = None) — skip them
  - Empty measurement response is valid — handle gracefully
  - Core pollutants only: pm25, pm10, no2, o3, co, so2
"""

import requests
import logging
import time

# Set up logging so every function call is traceable in Airflow logs
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openaq.org/v3"

# The pollutants we care about
# We discovered in exploration that stations also measure wind, humidity,
# temperature etc. We exclude those — they are weather metrics, not air
# quality metrics, and they would pollute (pun intended) our AQI analysis
CORE_POLLUTANTS = {"pm25", "pm10", "no2", "o3", "co", "so2"}


def get_headers(api_key: str) -> dict:
    """
    Build the auth headers required by OpenAQ v3.
    Every single request to the API must include these.
    """
    return {
        "X-API-Key": api_key,
        "Accept":    "application/json"
    }


def safe_request(url: str, headers: dict, params: dict, retries: int = 3) -> dict:
    """
    Make an HTTP GET request with retry logic.

    Why retry logic?
    APIs are not perfectly reliable. You will occasionally get:
      - 429: Rate limit exceeded (you sent too many requests too fast)
      - 500: Server error on their side (not your fault)
      - Timeout: Network hiccup

    Without retries, any of these would fail your entire DAG run.
    With retries, temporary issues recover automatically.

    We use exponential backoff: wait 2s, then 4s, then 8s between retries.
    This gives the API time to recover before we hammer it again.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30   # never wait more than 30s for a response
            )

            # 429 = rate limited — wait longer before retrying
            if response.status_code == 429:
                wait_time = 2 ** attempt  # 2, 4, 8 seconds
                logger.warning(
                    f"Rate limited by OpenAQ. "
                    f"Waiting {wait_time}s before retry {attempt}/{retries}"
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            logger.warning(f"Request timed out (attempt {attempt}/{retries}): {url}")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)

        except requests.exceptions.HTTPError as e:
            # 4xx errors (except 429) are our fault — don't retry
            if response.status_code < 500:
                logger.error(f"Client error {response.status_code}: {url}")
                raise
            # 5xx errors are server-side — retry
            logger.warning(f"Server error {response.status_code} (attempt {attempt}/{retries})")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)

    return {}


def fetch_country_id_map(api_key: str) -> dict:
    """
    Fetch all countries from OpenAQ and return a lookup map:
    { "NG": {"id": 100, "name": "Nigeria"}, "US": {...}, ... }

    Why we do this at runtime instead of hardcoding:
    OpenAQ occasionally adds new countries or changes IDs.
    Building the map fresh at the start of each DAG run means
    we never have stale data. The /countries call is fast and cheap.
    """
    logger.info("Fetching country ID map from OpenAQ")

    data = safe_request(
        url     = f"{BASE_URL}/countries",
        headers = get_headers(api_key),
        params  = {"limit": 200, "page": 1}
    )

    country_map = {}
    for country in data.get("results", []):
        iso_code   = country.get("code")
        numeric_id = country.get("id")
        name       = country.get("name")

        if iso_code and numeric_id:
            country_map[iso_code] = {
                "id":   numeric_id,
                "name": name
            }

    logger.info(f"Resolved {len(country_map)} countries from OpenAQ")
    return country_map


def fetch_locations(api_key: str, country_numeric_id: int) -> list:
    """
    Fetch all active monitoring stations for a given country.

    'Active' means: datetimeFirst is not None.
    We discovered in exploration that many stations are registered
    in OpenAQ but have never reported data. Querying their sensors
    every day would waste API quota and slow the pipeline down.

    We paginate through all results because a country like the US
    has hundreds of stations — they won't all fit on page 1.
    """
    logger.info(f"Fetching locations for country ID {country_numeric_id}")

    all_locations = []
    page          = 1
    page_size     = 100   # max allowed by OpenAQ v3

    while True:
        data = safe_request(
            url     = f"{BASE_URL}/locations",
            headers = get_headers(api_key),
            params  = {
                "countries_id": country_numeric_id,
                "limit":        page_size,
                "page":         page
            }
        )

        results = data.get("results", [])
        if not results:
            # No more pages — we have fetched everything
            break

        # Filter to active locations only
        # datetimeFirst = None means the station has never reported
        active = [
            loc for loc in results
            if loc.get("datetimeFirst") is not None
        ]

        all_locations.extend(active)
        logger.info(
            f"  Page {page}: {len(results)} locations fetched, "
            f"{len(active)} active"
        )

        # # Check if there are more pages
        # found = data.get("meta", {}).get("found", 0)
        # if page * page_size > found:
        #     break   # we have fetched all available pages

        page += 1
        time.sleep(0.5)   # be polite — don't hammer the API

    logger.info(
        f"Total active locations for country {country_numeric_id}: "
        f"{len(all_locations)}"
    )
    return all_locations


def fetch_daily_measurements(
    api_key:    str,
    sensor_id:  int,
    date_from:  str,
    date_to:    str
) -> list:
    """
    Fetch daily average measurements for a single sensor.

    date_from and date_to format: "YYYY-MM-DD"

    Returns a list of measurement records, or an empty list if the
    sensor has no data for the requested date range.
    Empty is valid — we discovered this in exploration with Nigeria.
    """
    data = safe_request(
        url     = f"{BASE_URL}/sensors/{sensor_id}/days",
        headers = get_headers(api_key),
        params  = {
            "date_from": date_from,
            "date_to":   date_to,
            "limit":     100
        }
    )

    measurements = data.get("results", [])
    logger.info(
        f"  Sensor {sensor_id}: {len(measurements)} measurements "
        f"for {date_from} to {date_to}"
    )
    return measurements


def extract_sensors(location: dict) -> list:
    """
    Extract only core pollutant sensors from a location record.

    We filter here rather than in the DAG to keep the DAG clean.
    We only want pm25, pm10, no2, o3, co, so2 — not weather metrics.
    """
    core_sensors = []

    for sensor in location.get("sensors", []):
        parameter = sensor.get("parameter", {})
        pollutant = parameter.get("name", "").lower()

        if pollutant in CORE_POLLUTANTS:
            core_sensors.append({
                "sensor_id":  sensor["id"],
                "pollutant":  pollutant,
                "units":      parameter.get("units"),
                "display_name": parameter.get("displayName")
            })

    return core_sensors
