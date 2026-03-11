from unittest.mock import MagicMock, patch

from etl.assets.extract_locations_bronze import run_locations_bronze


def _build_location(location_id: int, lat: float, lon: float) -> dict:
    return {
        "id": location_id,
        "name": f"Location {location_id}",
        "locality": "Test Locality",
        "country": {"code": "US", "name": "United States"},
        "coordinates": {"latitude": lat, "longitude": lon},
        "sensors": [{"id": location_id * 10, "name": "pm25"}],
        "timezone": "UTC",
        "isMobile": False,
        "datetimeFirst": {"utc": "2024-01-01T00:00:00Z"},
        "datetimeLast": {"utc": "2024-01-02T00:00:00Z"},
    }


@patch(
    "etl.assets.extract_locations_bronze.NominatimClient.reverse_geocode",
    return_value={"address": {"city": "Seattle", "state": "Washington"}},
)
@patch("etl.assets.extract_locations_bronze.OpenAQClient")
def test_run_locations_bronze_end_to_end_with_single_page(
    mock_openaq_class,
    _mock_reverse_geocode,
):
    openaq = MagicMock()
    mock_openaq_class.return_value = openaq
    openaq.get.side_effect = [
        {"results": [_build_location(1, 47.6062, -122.3321)]},
        {"results": []},
    ]

    df = run_locations_bronze(country_id=155)

    assert len(df) == 1
    assert df.loc[0, "location_id"] == 1
    assert df.loc[0, "city"] == "Seattle"
    assert df.loc[0, "state"] == "Washington"
    assert "latitude" not in df.columns
    assert "longitude" not in df.columns


@patch("etl.assets.extract_locations_bronze.time.sleep", return_value=None)
@patch(
    "etl.assets.extract_locations_bronze.NominatimClient.reverse_geocode",
    return_value={"address": {"city": "Test City", "state": "Test State"}},
)
@patch("etl.assets.extract_locations_bronze.OpenAQClient")
def test_run_locations_bronze_returns_all_rows_from_pagination(
    mock_openaq_class,
    _mock_reverse_geocode,
    _mock_sleep,
):
    openaq = MagicMock()
    mock_openaq_class.return_value = openaq

    page_1 = [_build_location(i, 40.0 + i, -70.0 - i) for i in range(1, 21)]
    page_2 = [_build_location(i, 40.0 + i, -70.0 - i) for i in range(21, 41)]

    openaq.get.side_effect = [
        {"results": page_1},
        {"results": page_2},
        {"results": []},
    ]

    df = run_locations_bronze(country_id=155)

    assert len(df) == 40
    assert df["location_id"].min() == 1
    assert df["location_id"].max() == 40
