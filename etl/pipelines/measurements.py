import os
import yaml
from loguru import logger
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from sqlalchemy import Column, Integer, String, Float, MetaData, Table
from dotenv import load_dotenv
from etl.assets.extract_daily_measurements_bronze import run_measurements_bronze
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
    logger.info("Starting measurements pipeline run")

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

    # extract measurements metadata from existing locations in DB
    logger.info("Extracting measurements metadata")

    datetime_to   = datetime.now(tz=timezone.utc)
    datetime_from = datetime_to - relativedelta(months=6)

    df_measurements = run_measurements_bronze(postgresql_client,datetime_from,datetime_to)
    
    # --- DEBUG: check dataframe shape and contents ---
    logger.info(f"Dataframe shape: {df_measurements.shape}")
    logger.info(f"Dataframe columns: {df_measurements.columns.tolist()}")
    logger.info(f"Dataframe dtypes:\n{df_measurements.dtypes}")

    # check what will actually be sent to postgres
    records = df_measurements.to_dict(orient="records")
    logger.info(f"Total records to load: {len(records)}")
    logger.info(f"First record: {records[0] if records else 'EMPTY'}")

    # show preview before loading
    logger.info(f"Extracted {len(df_measurements)} sensor records")
    if not df_measurements.empty:
        logger.info("Measurements dataframe head:\n" + df_measurements.head().to_string())

    # if config requests dry run, return early
    if config.get("dry_run", False):
        logger.info("Dry run requested, skipping load")
        return

# Define measurements table
    metadata = MetaData()
    measurements_table = Table(
        "measurements",    
        metadata,
        Column("sensor_id", Integer, primary_key=True),
        Column("period_datetime_from", String, primary_key=True),  # composite PK to avoid overwriting days
        Column("value", Float),
        Column("parameter_name", String),
        Column("parameter_id", Integer),
        Column("units", String),
        #Column("period_datetime_from", String),
        Column("period_datetime_to", String),
        Column("summary_min",Float),
        Column("summary_q02", Float),
        Column("summary_q25", Float),
        Column("summary_median", Float),
        Column("summary_q75", Float),
        Column("summary_q98", Float),
        Column("summary_max", Float),
        Column("summary_avg", Float),
        Column("summary_sd", Float),
        Column("location_id", Integer),
    )

    
    # --- DEBUG: try a single record insert first to isolate issues ---
    logger.info("Testing single record insert before full load...")
    try:
        test_record = df_measurements.head(1).to_dict(orient="records")
        logger.info(f"Test record: {test_record}")
        postgresql_client.insert(
            data=test_record,
            table=measurements_table,
            metadata=metadata,
        )
        logger.success("Test insert succeeded")
    except Exception as e:
        logger.error(f"Test insert failed: {e}")
        return


    logger.info("Loading measurements to bronze_layer")
    load(
        df=df_measurements,
        postgresql_client=postgresql_client,
        table=measurements_table,
        metadata=metadata,
        load_method=config.get("measurements_load_method", "upsert"),
    )

    logger.success("measurements pipeline run successful")


if __name__ == "__main__":
    with open("config/bronze_tables.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    pipeline({
        "measurements_load_method": cfg["bronze_tables"]["measurements"]["load_method"],
    })