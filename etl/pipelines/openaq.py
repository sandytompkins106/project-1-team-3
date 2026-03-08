import os
import yaml
from loguru import logger
from sqlalchemy import Column, Integer, String, MetaData, Table
from dotenv import load_dotenv
from etl.assets.extract_locations_bronze import run_locations_bronze
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
    logger.info("Starting bronze pipeline run")
    
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
    
    # Extract locations
    logger.info("Extracting locations from OpenAQ API")
    df_locations = run_locations_bronze(country_id=config.get("country_id", 155))
    
    # Define locations table
    metadata = MetaData()
    locations_table = Table(
        "locations",
        metadata,
        Column("location_id", Integer, primary_key=True),
        Column("name", String),
        Column("locality", String),
        Column("city", String),
        Column("state", String),
        Column("country_code", String),
        Column("country_name", String),
        Column("sensors", String),
        Column("timezone", String),
        Column("is_mobile", String),
        Column("first_updated_utc", String),
        Column("last_updated_utc", String),
    )
    
    # Load locations to bronze
    logger.info("Loading locations to bronze_layer")
    load(
        df=df_locations,
        postgresql_client=postgresql_client,
        table=locations_table,
        metadata=metadata,
        load_method=config.get("locations_load_method", "upsert"),
    )
    
    logger.success("Bronze pipeline run successful")


if __name__ == "__main__":
    # Load config
    with open("etl/config/bronze_tables.yaml", "r") as f:
        bronze_config = yaml.safe_load(f)
    
    # Run pipeline
    pipeline({
        "country_id": bronze_config["bronze_tables"]["locations"]["country_id"],
        "locations_load_method": bronze_config["bronze_tables"]["locations"]["load_method"],
    })