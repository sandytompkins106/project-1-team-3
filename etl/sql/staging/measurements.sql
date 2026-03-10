{% set config = {
    "source_table_name": "measurements",
    "load_method": "overwrite"
} %}

SELECT
    location_id,
    sensor_id,
    value,
    parameter_id,
    parameter_name,
    units,
    period_datetime_from,
    period_datetime_to,
    summary_min,
    summary_q02,
    summary_q25,
    summary_median,
    summary_q75,
    summary_q98,
    summary_max,
    summary_avg,
    summary_sd
FROM public.{{ config["source_table_name"] }}
ORDER BY
    location_id,
    sensor_id,
    value;
