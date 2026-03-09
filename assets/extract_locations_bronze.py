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

    return df_clean

def run_locations_bronze(country_id: int) -> pd.DataFrame:
    """
    Full bronze ingestion for locations.
    """
    raw = get_all_locations(country_id)
    df = build_locations_raw(raw)
    return df
