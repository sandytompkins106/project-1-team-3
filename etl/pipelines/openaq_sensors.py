import os
import yaml
from loguru import logger
from sqlalchemy import Column, Integer, Float, String, MetaData, Table
from dotenv import load_dotenv
from etl.assets.extract_sensors_bronze import run_sensors_bronze
from etl.db.postgresql_client import PostgreSqlClient


def load(
    df,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """
    Load dataframe to postgres using specified method.
    
    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: 'insert' (append), 'overwrite' (full), or 'upsert' (default)
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )


def pipeline(config: dict):
    logger.info("Starting bronze pipeline run [ sensors ]")
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Set up environment variables
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")
    
    # Create postgres client
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    
    # Extract sensors
    logger.info("Extracting sensors from OpenAQ API")

    df_sensors = run_sensors_bronze(postgresql_client)
    
    # Define sensors table
    metadata = MetaData()
    sensors_table = Table(
        "sensors",
        metadata,
        Column("sensor_id", Integer, primary_key=True),
        Column("name", String),
        Column("parameter_id", Integer),
        Column("parameter", String),
        Column("units", String),
        Column("parameter_display_name", String),
        Column("first_updated_utc", String),
        Column("last_updated_utc", String),
        Column("coverage_expected_count", Integer),
        Column("coverage_observed_count", Integer),
        Column("coverage_percent_complete", Float),
        Column("coverage_percent_coverage", Float),
        Column("coverage_from_utc", String),
        Column("coverage_to_utc", String),
        Column("latest_datetime_utc", String),
        Column("latest_value", Float),
        Column("latest_lat", Float),
        Column("latest_lon", Float),
        Column("summary_min", Float),
        Column("summary_max", Float),
        Column("summary_avg", Float),
        Column("summary_sd", Float),
        Column("location_id", Integer),
    )
    
    # Load sensors to bronze
    logger.info("Loading sensors to bronze_layer")
    load(
        df=df_sensors,
        postgresql_client=postgresql_client,
        table=sensors_table,
        metadata=metadata,
        load_method=config.get("sensors_load_method", "upsert"),
    )
    
    logger.success("Bronze pipeline run [ sensors ] successful")


if __name__ == "__main__":
    # Load config
    with open("etl/config/bronze_tables.yaml", "r") as f:
        bronze_config = yaml.safe_load(f)
    
    # Run pipeline
    pipeline({
        "sensors_load_method": bronze_config["bronze_tables"]["sensors"]["load_method"],
    })