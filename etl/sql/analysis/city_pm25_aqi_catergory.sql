{% set config = {
    "load_method": "overwrite",
    "target_table_name": "city_pm25_aqi_category",
    "months_back": 6,
    "parameter_name": "pm25"
} %}

-- Step 1: Pull raw PM2.5 readings per city for the last 6 months
WITH city_measurements AS (
    SELECT
        l.city,
        m.value::numeric AS pm25_value
    FROM measurements AS m
    JOIN sensors   AS s ON s.location_id = m.location_id AND s.sensor_id = m.sensor_id
    JOIN locations AS l ON l.location_id = m.location_id
    WHERE m.parameter_name = '{{ config["parameter_name"] }}'
      AND m.period_datetime_to::timestamptz >= NOW() - make_interval(months => {{ config["months_back"] }})
),

-- Step 2: Average the readings per city
city_averages AS (
    SELECT
        city,
        ROUND(AVG(pm25_value)::numeric, 2) AS avg_pm25,
        COUNT(*) AS sample_count
    FROM city_measurements
    GROUP BY city
)

-- Step 3: Add AQI category, sort order, and rank
SELECT
    city,
    avg_pm25,
    sample_count,

    -- AQI category based on EPA PM2.5 breakpoints (µg/m³)
    CASE
        WHEN avg_pm25 <=   9.0 THEN 'Good'
        WHEN avg_pm25 <=  35.4 THEN 'Moderate'
        WHEN avg_pm25 <=  55.4 THEN 'Unhealthy for Sensitive Groups'
        WHEN avg_pm25 <= 125.4 THEN 'Unhealthy'
        WHEN avg_pm25 <= 225.4 THEN 'Very Unhealthy'
        ELSE                        'Hazardous'
    END AS aqi_category,

    -- Numeric version of the category for easy sorting (1 = cleanest, 6 = worst)
    CASE
        WHEN avg_pm25 <=   9.0 THEN 1
        WHEN avg_pm25 <=  35.4 THEN 2
        WHEN avg_pm25 <=  55.4 THEN 3
        WHEN avg_pm25 <= 125.4 THEN 4
        WHEN avg_pm25 <= 225.4 THEN 5
        ELSE                        6
    END AS aqi_category_order,

    -- Rank cities from most to least polluted
    RANK() OVER (ORDER BY avg_pm25 DESC) AS pollution_rank,

    NOW() AS updated_at

FROM city_averages
ORDER BY pollution_rank, avg_pm25 DESC, city;