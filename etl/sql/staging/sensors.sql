{% set config = {
    "source_table_name": "sensors",
    "load_method": "overwrite"
} %}

SELECT
    location_id,
    sensor_id,
    name,
    parameter_id,
    parameter_name,
    units,
    parameter_display_name,
    first_updated_utc,
    last_updated_utc,
    coverage_expected_count,
    coverage_observed_count,
    coverage_percent_complete,
    coverage_percent_coverage,
    coverage_from_utc,
    coverage_to_utc,
    latest_datetime_utc,
    latest_value,
    latest_lat,
    latest_lon,
    summary_min,
    summary_max,
    summary_avg,
    summary_sd
FROM public.{{ config["source_table_name"] }}
ORDER BY
    location_id,
    sensor_id;
