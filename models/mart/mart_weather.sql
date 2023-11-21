SELECT DISTINCT
    location_key,
    dt,
    event_date,
    DATETIME_TRUNC(event_date, HOUR) as rounded_event_date,
    city_name,
    cloud_cover, 
    wind_deg, 
    wind_speed, 
    pressure,
    temp_c,
    humidity,
    visibility
FROM (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['stg_weather.dt', 'stg_weather.city_name']) }} as location_key,
        dt,
        TIMESTAMP_SECONDS(dt) as event_date,
        city_name,
        cloud_cover, 
        wind_deg, 
        wind_speed, 
        pressure,
        temp_k - 273 as temp_c,
        humidity,
        visibility
    FROM {{ ref('stg_weather') }}
) AS subquery

