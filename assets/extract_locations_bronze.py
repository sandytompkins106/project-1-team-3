import json
import time

import pandas as pd

from connectors.nominatim_client import NominatimClient
from connectors.openaq_client import OpenAQClient
from loguru import logger


def get_all_locations(country_id: int, page_size: int = 100) -> list:

    client = OpenAQClient()
    all_results = []
    page = 1

    while True:

        params = {
            "countries_id": country_id,
            "limit": page_size,
            "page": page,
            "order_by": "id",
            "sort_order": "desc"
        }

        response = client.get("locations", params=params)
        results = response.get("results", [])

        if not results:
            logger.info("Extraction complete.")
            break

        all_results.extend(results)
        page += 1

    return all_results


def clean_sensors_for_storage(raw_sensors) -> str:
    """Store sensors as a stable JSON string with only key fields needed downstream."""
    if not isinstance(raw_sensors, list):
        return "[]"

    cleaned = []
    for sensor in raw_sensors:
        if not isinstance(sensor, dict):
            continue

        sensor_id = sensor.get("id")
        if sensor_id is None:
            continue

        cleaned.append(
            {
                "id": sensor_id,
                "name": sensor.get("name"),
            }
        )

    # separators make stored JSON compact and deterministic.
    return json.dumps(cleaned, separators=(",", ":"))


def enrich_city_state_from_coordinates(
    df: pd.DataFrame,
    sleep_seconds: float = 1.0,
    log_every: int = 50,
) -> pd.DataFrame:
    """Add city/state columns via reverse geocoding with in-memory caching."""
    if df.empty:
        df["city"] = []
        df["state"] = []
        return df

    nominatim = NominatimClient()

    cache: dict[tuple[float, float], tuple[str | None, str | None]] = {}
    cities: list[str | None] = []
    states: list[str | None] = []
    api_calls = 0
    cache_hits = 0
    total_rows = len(df)

    logger.info(
        f"Nominatim start: rows={total_rows}, sleep_seconds={sleep_seconds}"
    )

    for idx, row in enumerate(df.itertuples(index=False), start=1):
        lat = getattr(row, "latitude", None)
        lon = getattr(row, "longitude", None)

        if pd.isna(lat) or pd.isna(lon):
            cities.append(None)
            states.append(None)
            continue

        # Rounding improves cache hits for nearby identical coordinates.
        key = (round(float(lat), 5), round(float(lon), 5))
        if key in cache:
            city, state = cache[key]
            cache_hits += 1
            cities.append(city)
            states.append(state)
            if log_every > 0 and idx % log_every == 0:
                logger.info(
                    f"Nominatim progress: {idx}/{total_rows} rows | api_calls={api_calls}, cache_hits={cache_hits}"
                )
            continue

        try:
            api_calls += 1
            result = nominatim.reverse_geocode(key[0], key[1])
            address = result.get("address", {}) if result else {}
            city, state = NominatimClient.extract_city_state(address)
        except Exception:
            city, state = None, None

        cache[key] = (city, state)
        cities.append(city)
        states.append(state)

        if log_every > 0 and idx % log_every == 0:
            logger.info(
                f"Nominatim progress: {idx}/{total_rows} rows | api_calls={api_calls}, cache_hits={cache_hits}"
            )

        # Respect Nominatim usage guidance.
        time.sleep(sleep_seconds)

    df["city"] = cities
    df["state"] = states
    logger.info(
        f"Nominatim complete: rows={total_rows}, api_calls={api_calls}, cache_hits={cache_hits}"
    )
    return df

def build_locations_raw(raw_results: list) -> pd.DataFrame:
    df = pd.json_normalize(raw_results)

    df_clean = df[[
        "id",
        "name",
        "locality",           
        "country.code",
        "country.name",
        "coordinates.latitude",
        "coordinates.longitude",
        "sensors",
        "timezone",
        "isMobile",
        "datetimeFirst.utc",
        "datetimeLast.utc"
    ]].copy()

    df_clean.columns = [
        "location_id",
        "name",
        "locality",
        "country_code",
        "country_name",
        "latitude",
        "longitude",
        "sensors",
        "timezone",
        "is_mobile",
        "first_updated_utc",
        "last_updated_utc"
    ]

    df_clean["sensors"] = df_clean["sensors"].apply(clean_sensors_for_storage)
    df_clean = enrich_city_state_from_coordinates(df_clean)
    
    # Drop lat/lon after enrichment is complete
    df_clean = df_clean.drop(columns=["latitude", "longitude"])
    
    return df_clean

def run_locations_bronze(country_id: int) -> pd.DataFrame:
    """
    Full bronze ingestion for locations.
    """
    raw = get_all_locations(country_id)
    return build_locations_raw(raw)

