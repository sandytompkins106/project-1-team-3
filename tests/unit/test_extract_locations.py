import json
from unittest.mock import MagicMock, patch

import pandas as pd

from etl.assets.extract_locations_bronze import (
	build_locations_raw,
	clean_sensors_for_storage,
	enrich_city_state_from_coordinates,
	get_all_locations,
)


def test_clean_sensors_for_storage_keeps_only_expected_fields():
	raw = [
		{"id": 101, "name": "pm25", "extra": "ignore"},
		{"name": "missing_id"},
		"not-a-dict",
		{"id": 102, "name": "pm10"},
	]

	result = clean_sensors_for_storage(raw)

	assert json.loads(result) == [
		{"id": 101, "name": "pm25"},
		{"id": 102, "name": "pm10"},
	]


@patch("etl.assets.extract_locations_bronze.OpenAQClient")
def test_get_all_locations_paginates_and_returns_all_rows(mock_client_class):
	mock_client = MagicMock()
	mock_client_class.return_value = mock_client

	page_1 = [{"id": i} for i in range(1, 21)]
	page_2 = [{"id": i} for i in range(21, 41)]
	mock_client.get.side_effect = [
		{"results": page_1},
		{"results": page_2},
		{"results": []},
	]

	results = get_all_locations(country_id=155, page_size=20)

	assert len(results) == 40
	assert results[0]["id"] == 1
	assert results[-1]["id"] == 40
	assert mock_client.get.call_count == 3


@patch(
	"etl.assets.extract_locations_bronze.NominatimClient.reverse_geocode",
	return_value={"address": {"city": "Seattle", "state": "Washington"}},
)
def test_enrich_city_state_from_coordinates_uses_cache(mock_reverse_geocode):

	df = pd.DataFrame(
		[
			{"latitude": 47.6062, "longitude": -122.3321},
			{"latitude": 47.6062, "longitude": -122.3321},
			{"latitude": None, "longitude": None},
		]
	)

	out = enrich_city_state_from_coordinates(df, sleep_seconds=0, log_every=0)

	assert out["city"].tolist() == ["Seattle", "Seattle", None]
	assert out["state"].tolist() == ["Washington", "Washington", None]
	assert mock_reverse_geocode.call_count == 1


@patch("etl.assets.extract_locations_bronze.enrich_city_state_from_coordinates")
def test_build_locations_raw_transforms_and_drops_lat_lon(mock_enrich):
	def passthrough_with_city_state(df, *_args, **_kwargs):
		df["city"] = ["Seattle"]
		df["state"] = ["Washington"]
		return df

	mock_enrich.side_effect = passthrough_with_city_state

	raw = [
		{
			"id": 10,
			"name": "Location A",
			"locality": "Downtown",
			"country": {"code": "US", "name": "United States"},
			"coordinates": {"latitude": 47.6062, "longitude": -122.3321},
			"sensors": [{"id": 1, "name": "pm25", "extra": "x"}],
			"timezone": "America/Los_Angeles",
			"isMobile": False,
			"datetimeFirst": {"utc": "2024-01-01T00:00:00Z"},
			"datetimeLast": {"utc": "2024-01-02T00:00:00Z"},
		}
	]

	df = build_locations_raw(raw)

	assert "latitude" not in df.columns
	assert "longitude" not in df.columns
	assert df.loc[0, "location_id"] == 10
	assert df.loc[0, "country_code"] == "US"
	assert json.loads(df.loc[0, "sensors"]) == [{"id": 1, "name": "pm25"}]
