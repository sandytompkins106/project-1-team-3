import pandas as pd
from connectors.openaq_client import OpenAQClient
from db.postgresql_client import PostgreSqlClient


def fetch_sensor_metadata(client: OpenAQClient, sensor_id: int, max_retries: int = 3):
    """Fetch metadata for a single sensor (OpenAQ v3) with simple retry logic."""
    # client.get already handles 429 with sleep, so just call it repeatedly
    for _ in range(max_retries):
        response = client.get(f"sensors/{sensor_id}", params={})
        results = response.get("results", [])
        return results
    return []


def build_sensors_raw(raw_results: list) -> pd.DataFrame:
    """Normalize raw metadata list into a clean DataFrame."""
    if not raw_results:
        return pd.DataFrame()

    df = pd.json_normalize(raw_results)
    # select typical columns if present
    columns = [
        "id",
        "name",
        "entity",
        "sourceType",
        "country",
        "city",
        "location",
        "sensorType",
        "manufacturer",
        "model",
        "unit",
        "lastUpdated",
    ]
    existing = [c for c in columns if c in df.columns]
    df_clean = df[existing].copy()
    new_names = [
        "sensor_id",
        "name",
        "entity",
        "source_type",
        "country",
        "city",
        "location_name",
        "sensor_type",
        "manufacturer",
        "model",
        "unit",
        "last_updated",
    ]
    df_clean.columns = new_names[: len(existing)]
    return df_clean


def run_sensors_bronze(postgres_client: PostgreSqlClient) -> pd.DataFrame:
    """Extract sensor metadata for every location already stored in bronze.locations."""
    loc_data = postgres_client.get_table("locations")
    if not loc_data:
        return pd.DataFrame()
    
    df_loc = pd.DataFrame(loc_data)

    client = OpenAQClient()
    all_meta = []

    for _, row in df_loc.iterrows():
        loc_id = row["location_id"]
        sensors = row.get("sensors", [])
        for s in sensors:
            sid = s.get("id")
            if sid is None:
                continue
            meta = fetch_sensor_metadata(client, sid)
            for item in meta:
                item["location_id"] = loc_id
                item["sensor_id"] = sid
                all_meta.append(item)

    return build_sensors_raw(all_meta)


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from db.postgresql_client import PostgreSqlClient

    # load env
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    df_sensors = run_sensors_bronze(postgresql_client)
    print(df_sensors)