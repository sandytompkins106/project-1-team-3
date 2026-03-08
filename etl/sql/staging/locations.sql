{% set config = {
    "source_table_name": "locations",
    "load_method": "upsert"
} %}

SELECT
    location_id,
    name,
    locality,
    city,
    state,
    country_code,
    country_name,
    sensors,
    timezone,
    is_mobile,
    first_updated_utc,
    last_updated_utc
FROM public.{{ config["source_table_name"] }};
