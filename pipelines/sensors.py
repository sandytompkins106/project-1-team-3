import os
import yaml
from loguru import logger
from sqlalchemy import Column, Integer, String, Float, MetaData, Table
from dotenv import load_dotenv
from assets.extract_sensors_bronze import run_sensors_bronze
from db.postgresql_client import PostgreSqlClient


def load(
    df,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """
    Load dataframe to postgres using specified method.
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
    logger.info("Starting sensors pipeline run")

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

    # extract sensors metadata from existing locations in DB
    logger.info("Extracting sensors metadata")
    df_sensors = run_sensors_bronze(postgresql_client)
    # show preview before loading
    logger.info(f"Extracted {len(df_sensors)} sensor records")
    if not df_sensors.empty:
        logger.info("Sensor dataframe head:\n" + df_sensors.head().to_string())
    # if config requests dry run, return early
    if config.get("dry_run", False):
        logger.info("Dry run requested, skipping load")
        return

    metadata = MetaData()
    sensors_table = Table(
        "sensors",
        metadata,
        Column("sensor_id", Integer, primary_key=True),
        Column("name", String),
        Column("entity", String),
        Column("source_type", String),
        Column("country", String),
        Column("city", String),
        Column("location_name", String),
        Column("sensor_type", String),
        Column("manufacturer", String),
        Column("model", String),
        Column("unit", String),
        Column("last_updated", String),
        Column("location_id", Integer),
    )

    logger.info("Loading sensors to bronze_layer")
    load(
        df=df_sensors,
        postgresql_client=postgresql_client,
        table=sensors_table,
        metadata=metadata,
        load_method=config.get("sensors_load_method", "upsert"),
    )

    logger.success("Sensors pipeline run successful")


if __name__ == "__main__":
    with open("config/bronze_tables.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    pipeline({
        "sensors_load_method": cfg["bronze_tables"]["sensors"]["load_method"],
    })