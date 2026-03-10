# OpenAQ ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for air quality data from the [OpenAQ](https://openaq.org/) API. It extracts locations, sensors, and daily measurements, transforms the data, and loads it into a PostgreSQL database hosted on Amazon RDS.

## Project Structure

- `etl/assets/`: Data extraction scripts
- `etl/config/`: Configuration files
- `etl/connectors/`: API clients (OpenAQ, Nominatim)
- `etl/db/`: Database client
- `etl/pipelines/`: Pipeline orchestration
- `etl/sql/`: SQL scripts for staging and analysis
- `test/`: Unit and integration tests

## Architecture Diagram

![ETL pipelines diagram](/project-1-team-3/openaq-etl-diagram-v2.png)