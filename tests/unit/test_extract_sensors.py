import json
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open
from sqlalchemy import Column, Integer, Float, String, MetaData, Table


# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

SENSOR_API_RESPONSE = {
    "id": 101,
    "name": "PM2.5",
    "parameter": {
        "id": 1,
        "name": "pm25",
        "units": "µg/m³",
        "displayName": "PM 2.5",
    },
    "datetimeFirst": {"utc": "2024-01-01T00:00:00Z"},
    "datetimeLast":  {"utc": "2024-06-01T00:00:00Z"},
    "coverage": {
        "expectedCount":   8760,
        "observedCount":   8000,
        "percentComplete": 91.3,
        "percentCoverage": 91.3,
        "datetimeFrom":    {"utc": "2024-01-01T00:00:00Z"},
        "datetimeTo":      {"utc": "2024-06-01T00:00:00Z"},
    },
    "latest": {
        "datetime":    {"utc": "2024-06-01T12:00:00Z"},
        "value":       12.5,
        "coordinates": {"latitude": -33.8688, "longitude": 151.2093},
    },
    "summary": {"min": 1.0, "max": 50.0, "avg": 12.5, "sd": 5.0},
}


def _normalised_response(location_id: int = 99) -> dict:
    """Return the flat dict that pd.json_normalize produces from SENSOR_API_RESPONSE."""
    flat = pd.json_normalize(SENSOR_API_RESPONSE, sep="_").to_dict(orient="records")[0]
    flat["location_id"] = location_id
    return flat


# ---------------------------------------------------------------------------
# Tests for extract_sensors_bronze.py
# ---------------------------------------------------------------------------

class TestGetAllSensors:
    """Tests for get_all_sensors()"""

    def _make_pg_client(self, sensors_value):
        """Build a mock PostgreSQL client whose execute_sql returns one row."""
        client = MagicMock()
        client.execute_sql.return_value = [
            {"location_id": 99, "sensors": sensors_value}
        ]
        return client

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_returns_list(self, mock_openaq_cls):
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_api = MagicMock()
        mock_api.get.return_value = {"results": [dict(SENSOR_API_RESPONSE)]}
        mock_openaq_cls.return_value = mock_api

        pg = self._make_pg_client([{"id": 101}])
        result = get_all_sensors(pg)

        assert isinstance(result, list)
        assert len(result) == 1

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_location_id_tagged(self, mock_openaq_cls):
        """Each result must carry the parent location_id."""
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_api = MagicMock()
        mock_api.get.return_value = {"results": [dict(SENSOR_API_RESPONSE)]}
        mock_openaq_cls.return_value = mock_api

        pg = self._make_pg_client([{"id": 101}])
        result = get_all_sensors(pg)

        assert result[0]["location_id"] == 99

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_sensors_as_json_string(self, mock_openaq_cls):
        """sensors column may arrive as a JSON string – must be parsed."""
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_api = MagicMock()
        mock_api.get.return_value = {"results": [dict(SENSOR_API_RESPONSE)]}
        mock_openaq_cls.return_value = mock_api

        pg = self._make_pg_client(json.dumps([{"id": 101}]))
        result = get_all_sensors(pg)

        assert len(result) == 1

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_multiple_sensors_per_location(self, mock_openaq_cls):
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_api = MagicMock()
        mock_api.get.return_value = {"results": [dict(SENSOR_API_RESPONSE)]}
        mock_openaq_cls.return_value = mock_api

        pg = MagicMock()
        pg.execute_sql.return_value = [
            {"location_id": 1, "sensors": [{"id": 101}, {"id": 102}]}
        ]

        result = get_all_sensors(pg)
        assert len(result) == 2  # one result per sensor call

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_empty_results_from_api(self, mock_openaq_cls):
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_api = MagicMock()
        mock_api.get.return_value = {"results": []}
        mock_openaq_cls.return_value = mock_api

        pg = self._make_pg_client([{"id": 101}])
        result = get_all_sensors(pg)

        assert result == []

    @patch("etl.assets.extract_sensors_bronze.OpenAQClient")
    def test_no_locations(self, mock_openaq_cls):
        from etl.assets.extract_sensors_bronze import get_all_sensors

        mock_openaq_cls.return_value = MagicMock()
        pg = MagicMock()
        pg.execute_sql.return_value = []

        result = get_all_sensors(pg)
        assert result == []


class TestBuildSensorsRaw:
    """Tests for build_sensors_raw()"""

    def _sample_df(self) -> pd.DataFrame:
        from etl.assets.extract_sensors_bronze import build_sensors_raw
        return build_sensors_raw([_normalised_response()])

    def test_returns_dataframe(self):
        df = self._sample_df()
        assert isinstance(df, pd.DataFrame)

    def test_expected_columns_present(self):
        expected = {
            "sensor_id", "name", "parameter_id", "parameter_name", "units",
            "parameter_display_name", "first_updated_utc", "last_updated_utc",
            "coverage_expected_count", "coverage_observed_count",
            "coverage_percent_complete", "coverage_percent_coverage",
            "coverage_from_utc", "coverage_to_utc",
            "latest_datetime_utc", "latest_value", "latest_lat", "latest_lon",
            "summary_min", "summary_max", "summary_avg", "summary_sd",
            "location_id",
        }
        df = self._sample_df()
        assert expected.issubset(set(df.columns))

    def test_row_count(self):
        from etl.assets.extract_sensors_bronze import build_sensors_raw
        raw = [_normalised_response(i) for i in range(5)]
        df = build_sensors_raw(raw)
        assert len(df) == 5

    def test_sensor_id_renamed(self):
        """Original 'id' column must be renamed to 'sensor_id'."""
        df = self._sample_df()
        assert "sensor_id" in df.columns
        assert "id" not in df.columns

    def test_units_renamed(self):
        df = self._sample_df()
        assert "units" in df.columns
        assert "parameter_units" not in df.columns

    def test_location_id_preserved(self):
        df = self._sample_df()
        assert df["location_id"].iloc[0] == 99

    def test_empty_input_returns_empty_df(self):
        from etl.assets.extract_sensors_bronze import build_sensors_raw
        with pytest.raises(Exception):
            # pd.json_normalize on [] then column selection will raise KeyError
            build_sensors_raw([])


class TestRunSensorsBronze:
    """Integration-style test for run_sensors_bronze()"""

    @patch("etl.assets.extract_sensors_bronze.get_all_sensors")
    @patch("etl.assets.extract_sensors_bronze.build_sensors_raw")
    def test_calls_both_helpers(self, mock_build, mock_get):
        from etl.assets.extract_sensors_bronze import run_sensors_bronze

        mock_get.return_value = [_normalised_response()]
        mock_build.return_value = pd.DataFrame([{"sensor_id": 101}])

        pg = MagicMock()
        result = run_sensors_bronze(pg)

        mock_get.assert_called_once_with(pg)
        mock_build.assert_called_once_with(mock_get.return_value)
        assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Tests for openaq_sensors.py  (load + pipeline)
# ---------------------------------------------------------------------------

class TestLoad:
    """Tests for the load() function."""

    def _make_args(self):
        pg = MagicMock()
        df = pd.DataFrame([{"sensor_id": 1}])
        metadata = MetaData()
        table = Table("sensors", metadata, Column("sensor_id", Integer, primary_key=True))
        return pg, df, table, metadata

    def test_insert_calls_insert(self):
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        load(df, pg, table, metadata, load_method="insert")
        pg.insert.assert_called_once()

    def test_upsert_calls_upsert(self):
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        load(df, pg, table, metadata, load_method="upsert")
        pg.upsert.assert_called_once()

    def test_overwrite_calls_overwrite(self):
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        load(df, pg, table, metadata, load_method="overwrite")
        pg.overwrite.assert_called_once()

    def test_invalid_load_method_raises(self):
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        with pytest.raises(Exception, match="correct load method"):
            load(df, pg, table, metadata, load_method="bad_method")

    def test_upsert_is_default(self):
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        load(df, pg, table, metadata)  # no load_method kwarg
        pg.upsert.assert_called_once()

    def test_data_passed_as_records(self):
        """The dict passed to the DB client must use orient='records'."""
        from etl.pipeline.openaq_sensors import load
        pg, df, table, metadata = self._make_args()
        load(df, pg, table, metadata, load_method="insert")
        call_kwargs = pg.insert.call_args[1]
        assert call_kwargs["data"] == df.to_dict(orient="records")


class TestPipeline:
    """Tests for the pipeline() function."""

    def _env(self):
        return {
            "SERVER_NAME":   "localhost",
            "DATABASE_NAME": "testdb",
            "DB_USERNAME":   "user",
            "DB_PASSWORD":   "pass",
            "PORT":          "5432",
        }

    @patch("etl.pipeline.openaq_sensors.load")
    @patch("etl.pipeline.openaq_sensors.run_sensors_bronze")
    @patch("etl.pipeline.openaq_sensors.PostgreSqlClient")
    @patch("etl.pipeline.openaq_sensors.load_dotenv")
    @patch.dict("os.environ", {
        "SERVER_NAME": "localhost", "DATABASE_NAME": "testdb",
        "DB_USERNAME": "user", "DB_PASSWORD": "pass", "PORT": "5432",
    })
    def test_pipeline_runs_successfully(
        self, mock_dotenv, mock_pg_cls, mock_extract, mock_load
    ):
        from etl.pipeline.openaq_sensors import pipeline

        mock_extract.return_value = pd.DataFrame([{"sensor_id": 1}])

        pipeline({"sensors_load_method": "upsert"})

        mock_extract.assert_called_once()
        mock_load.assert_called_once()

    @patch("etl.pipeline.openaq_sensors.load")
    @patch("etl.pipeline.openaq_sensors.run_sensors_bronze")
    @patch("etl.pipeline.openaq_sensors.PostgreSqlClient")
    @patch("etl.pipeline.openaq_sensors.load_dotenv")
    @patch.dict("os.environ", {
        "SERVER_NAME": "localhost", "DATABASE_NAME": "testdb",
        "DB_USERNAME": "user", "DB_PASSWORD": "pass", "PORT": "5432",
    })
    def test_pipeline_passes_load_method_from_config(
        self, mock_dotenv, mock_pg_cls, mock_extract, mock_load
    ):
        from etl.pipeline.openaq_sensors import pipeline

        mock_extract.return_value = pd.DataFrame([{"sensor_id": 1}])

        pipeline({"sensors_load_method": "overwrite"})

        _, kwargs = mock_load.call_args
        assert kwargs.get("load_method") == "overwrite"

    @patch("etl.pipeline.openaq_sensors.load")
    @patch("etl.pipeline.openaq_sensors.run_sensors_bronze")
    @patch("etl.pipeline.openaq_sensors.PostgreSqlClient")
    @patch("etl.pipeline.openaq_sensors.load_dotenv")
    @patch.dict("os.environ", {
        "SERVER_NAME": "localhost", "DATABASE_NAME": "testdb",
        "DB_USERNAME": "user", "DB_PASSWORD": "pass", "PORT": "5432",
    })
    def test_pipeline_default_load_method_is_upsert(
        self, mock_dotenv, mock_pg_cls, mock_extract, mock_load
    ):
        """If config key missing, load_method should default to 'upsert'."""
        from etl.pipeline.openaq_sensors import pipeline

        mock_extract.return_value = pd.DataFrame([{"sensor_id": 1}])

        pipeline({})  # empty config

        _, kwargs = mock_load.call_args
        assert kwargs.get("load_method") == "upsert"

    @patch("etl.pipeline.openaq_sensors.load")
    @patch("etl.pipeline.openaq_sensors.run_sensors_bronze")
    @patch("etl.pipeline.openaq_sensors.PostgreSqlClient")
    @patch("etl.pipeline.openaq_sensors.load_dotenv")
    @patch.dict("os.environ", {
        "SERVER_NAME": "localhost", "DATABASE_NAME": "testdb",
        "DB_USERNAME": "user", "DB_PASSWORD": "pass", "PORT": "5432",
    })
    def test_pipeline_creates_pg_client_with_env_vars(
        self, mock_dotenv, mock_pg_cls, mock_extract, mock_load
    ):
        from etl.pipeline.openaq_sensors import pipeline

        mock_extract.return_value = pd.DataFrame([{"sensor_id": 1}])
        pipeline({"sensors_load_method": "upsert"})

        mock_pg_cls.assert_called_once_with(
            server_name="localhost",
            database_name="testdb",
            username="user",
            password="pass",
            port="5432",
        )
