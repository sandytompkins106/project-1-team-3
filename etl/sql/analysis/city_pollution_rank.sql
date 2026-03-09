{% set config = {
    "load_method": "overwrite",
    "target_table_name": "city_pollution_rank",
    "months_back": 6,
    "parameter_name": "pm25"
} %}

WITH city_measurements AS (
    SELECT
        l.city,
        ROUND(m.value::numeric, 2) AS pollution_value
    FROM measurements AS m
    JOIN sensors AS s
        ON s.location_id = m.location_id
       AND s.sensor_id = m.sensor_id
    JOIN locations AS l
        ON l.location_id = m.location_id
    WHERE m.parameter_name = '{{ config["parameter_name"] }}'
      AND m.period_datetime_to::timestamptz >= NOW() - make_interval(months => {{ config["months_back"] }})
)
SELECT
    city,
    ROUND(AVG(pollution_value)::numeric, 2) AS avg_pm25,
    COUNT(*) AS sample_count,
    RANK() OVER (ORDER BY AVG(pollution_value) DESC) AS pollution_rank
FROM city_measurements
GROUP BY city
ORDER BY
    pollution_rank,
    avg_pm25 DESC,
    city;
