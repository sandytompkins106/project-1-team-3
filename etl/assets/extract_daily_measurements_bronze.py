import pandas as pd
from datetime import datetime
from etl.connectors.openaq_client import OpenAQClient
from etl.db.postgresql_client import PostgreSqlClient
from loguru import logger
import json


def get_all_measurements_data(
    sensor_id: int,
    datetime_from: datetime,
    datetime_to: datetime,
    page_size: int = 1000,
    max_retries: int=3,
) -> list:
    """
    Fetch all daily measurements for a sensor within a date range.
    Paginates through the OpenAQ API until no further results are returned.
    """

    client = OpenAQClient()
    all_results = []
    endpoint = f"sensors/{sensor_id}/measurements/daily"
    page = 1

    while True:
        params = {
            "datetime_from": datetime_from.isoformat(),
            "datetime_to":   datetime_to.isoformat(),
            "limit":         page_size,
            "page":          page,
        }

        response = client.get(endpoint, params=params)
        results = response.get("results", [])

        if not results:
            logger.info(
                f"Extraction complete for sensor {sensor_id} | "
                f"{datetime_from.date()} → {datetime_to.date()} | "
                f"Total records: {len(all_results)}"
            )
            break

        all_results.extend(results)
        page += 1

    return all_results


def build_measurements_data_raw(raw_results: list, sensor_id: int) -> pd.DataFrame:

    """Flatten and rename columns from raw daily measurements, filtered to PM2.5 only.
       Flattening — nested dicts (parameter, period, summary) are collapsed into a 
       single flat row. """
    
    if not raw_results:
        return pd.DataFrame()

    df = pd.DataFrame([
        {
            # core fields
            'sensor_id':            sensor_id,
            'value':                r['value'],
            'parameter_id':         r['parameter']['id'],
            'parameter_name':       r['parameter']['name'],
            'units':                r['parameter']['units'],
            # period
            'period_datetime_from': r['period']['datetimeFrom']['utc'],
            'period_datetime_to':   r['period']['datetimeTo']['utc'],
            # summary stats
            'summary_min':          r.get('summary', {}).get('min'),
            'summary_q02':          r.get('summary', {}).get('q02'),
            'summary_q25':          r.get('summary', {}).get('q25'),
            'summary_median':       r.get('summary', {}).get('median'),
            'summary_q75':          r.get('summary', {}).get('q75'),
            'summary_q98':          r.get('summary', {}).get('q98'),
            'summary_max':          r.get('summary', {}).get('max'),
            'summary_avg':          r.get('summary', {}).get('avg'),
            'summary_sd':           r.get('summary', {}).get('sd'),
        }
        for r in raw_results
        if r['parameter']['name'] == 'pm25'
    ])

    return df


def run_measurements_bronze(
    postgres_client: PostgreSqlClient,
    datetime_from: datetime,
    datetime_to: datetime,
) -> pd.DataFrame:
    """
    Orchestrate bronze-layer ingestion of daily PM2.5 measurements.
    Reads all locations from PostgreSQL, iterates over each sensor, and fetches
    measurements from the OpenAQ API, returning a single combined DataFrame.
    """
    # load all locations from bronze layer 
    loc_data = postgres_client.get_table("locations")
    if not loc_data:
        logger.warning("No locations found in database.")
        return pd.DataFrame()

    df_locations = pd.DataFrame(loc_data)
    all_measurements = []

    for _, row in df_locations.iterrows():
        location_id = row["location_id"]
        sensors     = row.get("sensors", [])

        # postgres returns JSON columns as strings, parse back to list
        if isinstance(sensors, str):
            sensors = json.loads(sensors)

        for sensor in sensors:
            sensor_id = sensor.get("id")
            if sensor_id is None:
                continue

            logger.info(f"Fetching measurements for sensor {sensor_id} | location {location_id}")

            raw = get_all_measurements_data(sensor_id, datetime_from, datetime_to)
            df  = build_measurements_data_raw(raw, sensor_id)

            if not df.empty:
                df["location_id"] = location_id  # tag with location for traceability
                all_measurements.append(df)

    if not all_measurements:
        logger.warning("No measurements data returned for any sensor.")
        return pd.DataFrame()

    return pd.concat(all_measurements, ignore_index=True)