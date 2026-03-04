# Project Status - OpenAQ Data Pipeline

**Last Updated:** March 4, 2026

## 🎯 Project Overview
Building a multi-layer data pipeline (Bronze → Gold) that extracts air quality data from OpenAQ API, loads to PostgreSQL RDS, and transforms for analytics. 

**Architecture:**
- **Bronze Layer:** Raw API data loaded to `bronze_layer` database
  - `locations` table: Air quality monitoring station metadata
  - `sensors` table: Sensor metadata linked to locations
  - `measurements` table: Time-series air quality measurements (TODO)
- **Gold Layer:** Cleansed, transformed data in `gold_layer` database (TODO)

---

## ✅ Completed Tasks

### 1. Set up Postgres Connection Utilities
- **File:** `db/postgresql_client.py`
- **Status:** ✅ Complete
- **Details:**
  - `PostgreSqlClient` class with methods: `insert()`, `overwrite()`, `upsert()`
  - `upsert()` includes chunking (default 1000 rows) to avoid pg8000 parameter limits
  - `get_table()` method uses SQLAlchemy to reflect and read tables as list[dict]
  - Handles environment variables from `.env`

### 2. Load Locations Data to Bronze Layer
- **File:** `pipelines/openaq.py`
- **Status:** ✅ Complete & Tested
- **Details:**
  - Extracts locations from OpenAQ API for country_id=155 (US)
  - Upserts to `bronze_layer.locations` table
  - Loads ~100+ rows per API call, paginated
  - Ready for incremental/full/upsert load modes

### 3. Fix & Complete extract_sensors_bronze.py
- **File:** `assets/extract_sensors_bronze.py`
- **Status:** ✅ Complete
- **Details:**
  - Reads locations from `bronze.locations` table (via PostgreSQL)
  - Iterates through sensors attached to each location
  - Fetches metadata from OpenAQ sensors/{id} endpoint
  - Normalizes and cleans columns (id→sensor_id, etc)
  - Returns DataFrame with location_id link
  - `__main__` block for standalone testing

### 4. Create YAML Config Files
- **File:** `config/bronze_tables.yaml`
- **Status:** ✅ Complete
- **Details:**
  - Defines table schemas, primary keys, load methods
  - Default load_method: `upsert` for all tables
  - Includes country_id and extract_function references
  - Ready for Jinja templating later

---

## 🔄 In Progress

### Load Sensors Data to Bronze Layer
- **Status:** ⏳ In Testing
- **Files:** `pipelines/sensors.py` (separate for testing)
- **Next Steps:**
  1. Run `python -m pipelines.sensors` to verify DataFrame output
  2. Check sensor table structure and data quality
  3. Confirm upsert chunking works with sensor payloads
  4. Merge back into unified `openaq.py` pipeline

---

## 📋 To Do

### Phase 1: Complete Bronze Layer (3 tables)

[ ] Load Sensors Data to Bronze Layer (in testing)
- Current blocking issue: need to verify sensor DataFrame before loading
- Once verified, can run `python -m pipelines.sensors` to load

[ ] Create extract_measurements_bronze.py
- Read sensors from bronze.sensors table
- Query measurements from OpenAQ API for each sensor
- Handle time-range pagination
- Normalize and clean output

[ ] Load Measurements Data to Bronze Layer
- Add measurements table schema to pipeline
- Extend bronz_tables.yaml config
- Test upsert with large time-series dataset

[ ] Implement Load Modes (full/incremental/upsert) for Bronze
- Currently hardcoded to upsert
- Need to expose via config or CLI flags
- Incremental mode: upsert only rows where timestamp > max_existing
- Full mode: drop and reload
- Upsert mode: update existing, insert new

### Phase 2: Configuration & Templating

[ ] Create Jinja Templates for Bronze Loaders
- Parametrize SQL generation
- Template table creation DDL
- Template upsert logic

[ ] Enhance YAML Configs
- Add timestamp_column for incremental mode
- Add chunk_size settings
- Add retry logic configs

### Phase 3: Gold Layer (Transformations)

[ ] Create Gold Layer Transformation Logic
- SQL transformations (dimensions, facts)
- Cleansing and deduplication
- Aggregations and rollups

[ ] Create YAML Config & Jinja Templates for Gold
- Mirror bronze pattern for gold layer
- Define dependencies between transformations

[ ] Create Gold Layer Loaders
- Load modes for gold tables
- Audit logging of transformations

### Phase 4: Orchestration

[ ] Wire Everything into Pipeline Orchestration
- Unified command to run all 3 bronze extracts sequentially
- Dependency tracking (sensors depends on locations, measurements depends on sensors)
- Error handling and retry logic
- Logging and monitoring

---

## 🛠️ Current Architecture

```
project-1-team-3/
├── .env                          # RDS credentials (SERVER_NAME, DB_USERNAME, etc)
├── config/
│   ├── config.py                 # API key config (dotenv)
│   └── bronze_tables.yaml        # Table schemas, load methods
├── connectors/
│   └── openaq_client.py          # OpenAQ API wrapper with retry logic
├── assets/
│   ├── extract_locations_bronze.py    # ✅ Locations extraction
│   └── extract_sensors_bronze.py      # ✅ Sensors extraction
├── db/
│   └── postgresql_client.py      # ✅ SQLAlchemy wrapper with upsert
├── pipelines/
│   ├── openaq.py                 # ✅ Locations → Bronze
│   └── sensors.py                # ⏳ Sensors → Bronze (testing)
└── data/
    └── locations_bronze2.csv     # Sample output
```

---

## 🔧 Environment Setup

### Required Environment Variables (`.env`)
```env
OPENAQ_API_KEY=<your-api-key>
OPENAQ_BASE_URL=https://api.openaq.org/v3
SERVER_NAME=<your-rds-endpoint>
DATABASE_NAME=bronze_layer
DB_USERNAME=postgres
DB_PASSWORD=<your-password>
PORT=5432
```

### Conda Environment
```bash
conda activate binance
pip install loguru python-dotenv pyyaml sqlalchemy pg8000
```

---

## ✨ Key Design Decisions

1. **Chunking for Upsert:**
   - pg8000 has a 65535 parameter limit per statement
   - Solution: batch data into 1000-row chunks, upsert each chunk separately
   - Applied to `PostgreSqlClient.upsert()`

2. **Separate Pipelines for Testing:**
   - `openaq.py`: locations only (proven working)
   - `sensors.py`: separate for testing before merging
   - Will reintegrate into single unified pipeline

3. **SQLAlchemy for Table Reading:**
   - `get_table()` uses SQLAlchemy reflection instead of pandas
   - Returns `list[dict]`, caller converts to DataFrame
   - Keeps DB layer pure SQLAlchemy

4. **YAML-Driven Configuration:**
   - All table metadata in `bronze_tables.yaml`
   - Load methods configurable per table
   - Easier to extend with Jinja later

---

## 🚀 Next Immediate Steps (Session 2)

1. **Verify sensors extraction output:**
   ```bash
   python -m assets.extract_sensors_bronze
   ```
   
2. **Run sensors pipeline with dry-run:**
   ```bash
   python -m pipelines.sensors
   ```
   
3. **Check bronze.sensors table in RDS:**
   - Row count
   - Column data types
   - Sample rows

4. **If verified, start measurements extraction:**
   - Follow same pattern as sensors
   - Create `extract_measurements_bronze.py`
   - Query measurements by sensor_id with date range

---

## 📝 Notes

- All three bronze tables use `upsert` load method by default
- Primary keys defined in `bronze_tables.yaml`: 
  - locations: `[location_id]`
  - sensors: `[sensor_id]`
  - measurements: `[location_id, sensor_id, timestamp]` (TODO)
- API client (`OpenAQClient`) already handles rate limiting (429) with exponential backoff
- Logging via `loguru` for better traceability

---

**Questions/Blockers:** None currently. Ready to proceed with sensors testing on next session.
