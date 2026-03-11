import sys
import yaml
from loguru import logger

import etl.pipelines.openaq_locations as locations_pipeline
import etl.pipelines.openaq_sensors as sensors_pipeline
import etl.pipelines.openaq_daily_measurements as measurements_pipeline
import etl.pipelines.gold_load as gold_load_pipeline


def load_config(path: str = "etl/config/bronze_tables.yaml") -> dict:
    """Load and parse the YAML config file from the given path."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    """Orchestrate the full OpenAQ ETL pipeline, including gold-layer materialization."""
    logger.info("=== Starting full OpenAQ ETL pipeline ===")

    try:
        cfg = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        sys.exit(1)

    bronze = cfg["bronze_tables"]

    # --- 1. Locations ---
    logger.info("--- Step 1/4: Locations ---")
    try:
        locations_pipeline.pipeline({
            "country_id": bronze["locations"]["country_id"],
            "locations_load_method": bronze["locations"]["load_method"],
        })
    except Exception as e:
        logger.error(f"Locations pipeline failed: {e}")
        sys.exit(1)

    # --- 2. Sensors ---
    logger.info("--- Step 2/4: Sensors ---")
    try:
        sensors_pipeline.pipeline({
            "sensors_load_method": bronze["sensors"]["load_method"],
        })
    except Exception as e:
        logger.error(f"Sensors pipeline failed: {e}")
        sys.exit(1)

    # --- 3. Measurements ---
    logger.info("--- Step 3/4: Measurements ---")
    try:
        measurements_pipeline.pipeline({
            "measurements_load_method": bronze["measurements"]["load_method"],
        })
    except Exception as e:
        logger.error(f"Measurements pipeline failed: {e}")
        sys.exit(1)

    # --- 4. Gold Load ---
    logger.info("--- Step 4/4: Gold Load ---")
    try:
        gold_load_pipeline.pipeline()
    except Exception as e:
        logger.error(f"Gold load pipeline failed: {e}")
        sys.exit(1)

    logger.success("=== Full OpenAQ ETL pipeline completed successfully ===")


if __name__ == "__main__":
    main()