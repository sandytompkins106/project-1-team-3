import json
import pandas as pd
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

    return df_clean

def run_locations_bronze(country_id: int) -> pd.DataFrame:
    """
    Full bronze ingestion for locations.
    """
    raw = get_all_locations(country_id)
    df = build_locations_raw(raw)
    return df
