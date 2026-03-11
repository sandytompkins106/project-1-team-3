import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from etl.assets.extract_daily_measurements_bronze import (
    get_all_measurements_data,
    build_measurements_data_raw,
)


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────

@pytest.fixture
def sample_measurements():
    """Raw API response with one PM2.5 and one PM10 record."""
    return [
        {
            "value": 12.5,
            "parameter": {
                "id": 1,
                "name": "pm25",
                "units": "µg/m³"
            },
            "period": {
                "datetimeFrom": {"utc": "2024-01-01T00:00:00Z"},
                "datetimeTo":   {"utc": "2024-01-02T00:00:00Z"}
            },
            "summary": {
                "min":    5.0,
                "q02":    6.0,
                "q25":    8.0,
                "median": 12.0,
                "q75":    15.0,
                "q98":    20.0,
                "max":    25.0,
                "avg":    12.5,
                "sd":     3.2,
            }
        },
        {
            "value": 20.0,
            "parameter": {
                "id": 2,
                "name": "pm10",   # should be filtered OUT
                "units": "µg/m³"
            },
            "period": {
                "datetimeFrom": {"utc": "2024-01-01T00:00:00Z"},
                "datetimeTo":   {"utc": "2024-01-02T00:00:00Z"}
            },
            "summary": {}
        },
    ]


@pytest.fixture
def expected_dataframe():
    """Expected DataFrame after build_measurements_data_raw — PM2.5 only."""
    return pd.DataFrame([
        {
            "sensor_id":            123,
            "value":                12.5,
            "parameter_id":         1,
            "parameter_name":       "pm25",
            "units":                "µg/m³",
            "period_datetime_from": "2024-01-01T00:00:00Z",
            "period_datetime_to":   "2024-01-02T00:00:00Z",
            "summary_min":          5.0,
            "summary_q02":          6.0,
            "summary_q25":          8.0,
            "summary_median":       12.0,
            "summary_q75":          15.0,
            "summary_q98":          20.0,
            "summary_max":          25.0,
            "summary_avg":          12.5,
            "summary_sd":           3.2,
        }
    ])


# ─────────────────────────────────────────────
# build_measurements_data_raw
# ─────────────────────────────────────────────

def test_build_measurements_data_raw_filters_pm25_only(sample_measurements, expected_dataframe):
    """
    Test that build_measurements_data_raw:
    - Filters out non-PM2.5 records (e.g. pm10)
    - Extracts all correct fields including summary stats
    - Returns a DataFrame matching the expected schema and values
    """
    df = build_measurements_data_raw(sample_measurements, sensor_id=123)

    assert len(df) == 1, "Should only contain 1 PM2.5 record, pm10 must be filtered out"
    assert (df["parameter_name"] == "pm25").all()

    pd.testing.assert_frame_equal(
        df.reset_index(drop=True),
        expected_dataframe.reset_index(drop=True),
        check_dtype=False,
    )


def test_build_measurements_data_raw_empty_input():
    """
    Test that build_measurements_data_raw returns an empty DataFrame
    when given an empty list.
    """
    df = build_measurements_data_raw([], sensor_id=123)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_build_measurements_data_raw_no_pm25():
    """
    Test that build_measurements_data_raw returns an empty DataFrame
    when none of the records are PM2.5.
    """
    raw = [
        {
            "value": 20.0,
            "parameter": {"id": 2, "name": "pm10", "units": "µg/m³"},
            "period": {
                "datetimeFrom": {"utc": "2024-01-01T00:00:00Z"},
                "datetimeTo":   {"utc": "2024-01-02T00:00:00Z"}
            },
            "summary": {}
        }
    ]
    df = build_measurements_data_raw(raw, sensor_id=123)

    assert df.empty, "DataFrame should be empty when no PM2.5 records exist"


# ─────────────────────────────────────────────
# get_all_measurements_data
# ─────────────────────────────────────────────

@patch("etl.assets.extract_daily_measurements_bronze.OpenAQClient")
def test_get_all_measurements_data_pagination(mock_client_class):
    """
    Test that get_all_measurements_data:
    - Iterates through all pages until an empty result is returned
    - Concatenates records from multiple pages into a single list
    """
    mock_instance = MagicMock()
    mock_client_class.return_value = mock_instance

    mock_instance.get.side_effect = [
        {"results": [{"value": 10}]},
        {"results": [{"value": 20}]},
        {"results": []},   # signals end of pagination
    ]

    results = get_all_measurements_data(
        sensor_id=123,
        datetime_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime_to=datetime(2024, 1, 31, tzinfo=timezone.utc),
    )

    assert len(results) == 2, "Should return 2 records across 2 pages"
    assert mock_instance.get.call_count == 3, "Should call API 3 times (2 pages + 1 empty)"


@patch("etl.assets.extract_daily_measurements_bronze.OpenAQClient")
def test_get_all_measurements_data_empty_response(mock_client_class):
    """
    Test that get_all_measurements_data returns an empty list
    when the API returns no results on the first call.
    """
    mock_instance = MagicMock()
    mock_client_class.return_value = mock_instance
    mock_instance.get.return_value = {"results": []}

    results = get_all_measurements_data(
        sensor_id=123,
        datetime_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime_to=datetime(2024, 1, 31, tzinfo=timezone.utc),
    )

    assert results == []