import pytest
import pandas as pd
from etl.assets.sensors_gold import get_us_aqi_category, transform


# ---------------------------------------------------------------------------
# Tests for get_us_aqi_category()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("pm25_value, expected_category", [
    (0.0,   "Good"),
    (5.0,   "Good"),
    (9.0,   "Good"),
    (9.1,   "Moderate"),
    (20.0,  "Moderate"),
    (35.4,  "Moderate"),
    (35.5,  "Unhealthy for Sensitive Groups"),
    (50.0,  "Unhealthy for Sensitive Groups"),
    (55.4,  "Unhealthy for Sensitive Groups"),
    (55.5,  "Unhealthy"),
    (100.0, "Unhealthy"),
    (125.4, "Unhealthy"),
    (125.5, "Very Unhealthy"),
    (200.0, "Very Unhealthy"),
    (225.4, "Very Unhealthy"),
    (225.5, "Hazardous"),
    (300.0, "Hazardous"),
    (-1.0,  "Invalid"),
])
def test_get_us_aqi_category(pm25_value, expected_category):
    assert get_us_aqi_category(pm25_value) == expected_category


# ---------------------------------------------------------------------------
# Tests for transform()
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    return pd.DataFrame([
        {"sensor_id": 42, "value": 5.0,   "parameter_name": "pm25"},
        {"sensor_id": 42, "value": 20.0,  "parameter_name": "pm25"},
        {"sensor_id": 42, "value": 40.0,  "parameter_name": "pm25"},
        {"sensor_id": 42, "value": 80.0,  "parameter_name": "pm25"},
        {"sensor_id": 42, "value": 150.0, "parameter_name": "pm25"},
        {"sensor_id": 42, "value": 250.0, "parameter_name": "pm25"},
    ])


@pytest.fixture
def expected_df(sample_df):
    df = sample_df.copy()
    df["aqi_category"] = [
        "Good",
        "Moderate",
        "Unhealthy for Sensitive Groups",
        "Unhealthy",
        "Very Unhealthy",
        "Hazardous",
    ]
    return df


def test_transform_adds_aqi_category_column(sample_df, expected_df):
    """Should add aqi_category column with correct values for each row."""
    df = transform(sample_df)
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)


def test_transform_empty_dataframe():
    """Should return empty DataFrame unchanged."""
    df = transform(pd.DataFrame())
    assert df.empty


def test_transform_does_not_modify_original(sample_df):
    """Should not mutate the input DataFrame."""
    original = sample_df.copy()
    transform(sample_df)
    pd.testing.assert_frame_equal(sample_df, original)
