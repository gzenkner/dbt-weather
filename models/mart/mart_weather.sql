SELECT DISTINCT
    location_key,
    unix_ts,
    event_date,
    name,
    clouds, 
    rain_per_hour,
    wind_gust,
    wind_deg, 
    wind_speed, 
    pressure,
    temp_celsius,
    humidity,
    visibility
FROM (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['stg_weather.dt', 'stg_weather.name']) }} as location_key,
        dt as unix_ts,
        TIMESTAMP_SECONDS(dt) as event_date,
        name,
        clouds, 
        COALESCE(rain_per_hour, 0) as rain_per_hour,
        COALESCE(wind_gust, 0) as wind_gust,
        wind_deg, 
        wind_speed, 
        pressure,
        temp - 273 as temp_celsius,
        humidity,
        visibility
    FROM {{ ref('stg_weather') }}
) AS subquery

