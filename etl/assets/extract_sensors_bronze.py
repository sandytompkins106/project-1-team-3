import json
import pandas as pd
from etl.connectors.openaq_client import OpenAQClient
from etl.db.postgresql_client import PostgreSqlClient
from loguru import logger


def get_all_sensors(postgresql_client: PostgreSqlClient) -> list:
    """
    Fetch full sensor details from the OpenAQ API for every sensor in the locations table.
    Tags each result with its originating location_id for downstream traceability.
    """

    client = OpenAQClient()
    all_results = []

    # Each row has a 'sensors' column containing a list of sensor objects
    rows = postgresql_client.execute_sql("SELECT location_id, sensors FROM locations")

    for row in rows:
        location_id = row["location_id"]
        sensors = row["sensors"]

        # Parse if the column comes back as a JSON string
        if isinstance(sensors, str):
            sensors = json.loads(sensors)

        # Each sensor in the list should have an 'id'
        for sensor in sensors:
            sensor_id = sensor["id"]

            params = {}

            response = client.get(f"sensors/{sensor_id}", params=params)
            results = response.get("results", [])

            # Tag each sensor row with the location id
            for result in results:
                result["location_id"] = location_id
            
            all_results.extend(results)

    return all_results

def build_sensors_raw(raw_results: list) -> pd.DataFrame:
    """
    Normalise raw sensor records, select relevant columns, and apply snake_case renaming.
    """

    df = pd.json_normalize(raw_results, sep="_")

    df_clean = df[[
        'id',
        'name',
        'parameter_id',
        'parameter_name',
        'parameter_units',
        'parameter_displayName',
        'datetimeFirst_utc',
        'datetimeLast_utc',
        'coverage_expectedCount',
        'coverage_observedCount',
        'coverage_percentComplete',
        'coverage_percentCoverage',
        'coverage_datetimeFrom_utc',
        'coverage_datetimeTo_utc',
        'latest_datetime_utc',
        'latest_value',
        'latest_coordinates_latitude',
        'latest_coordinates_longitude',
        'summary_min',
        'summary_max',
        'summary_avg',
        'summary_sd',
        'location_id',
    ]].rename(columns={
        'id':                           'sensor_id',
        'parameter_units':              'units',
        'parameter_displayName':        'parameter_display_name',
        'datetimeFirst_utc':            'first_updated_utc',
        'datetimeLast_utc':             'last_updated_utc',
        'coverage_expectedCount':       'coverage_expected_count',
        'coverage_observedCount':       'coverage_observed_count',
        'coverage_percentComplete':     'coverage_percent_complete',
        'coverage_percentCoverage':     'coverage_percent_coverage',
        'coverage_datetimeFrom_utc':    'coverage_from_utc',
        'coverage_datetimeTo_utc':      'coverage_to_utc',
        'latest_coordinates_latitude':  'latest_lat',
        'latest_coordinates_longitude': 'latest_lon',
    }).copy()

    return df_clean

def run_sensors_bronze(postgresql_client: PostgreSqlClient) -> pd.DataFrame:
    """Fetch and process all raw sensor records into a clean bronze layer DataFrame."""

    raw = get_all_sensors(postgresql_client)
    df = build_sensors_raw(raw)
    return df