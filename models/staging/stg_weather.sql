SELECT 
    location_time_key,
    dt,
    event_date,
    rounded_event_date,
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
        {{ dbt_utils.generate_surrogate_key(['DATETIME_TRUNC(TIMESTAMP_SECONDS(dt), HOUR)', 'raw_weather.city_name']) }} as location_time_key,
        dt,
        TIMESTAMP_SECONDS(dt) as event_date,
        DATETIME_TRUNC(TIMESTAMP_SECONDS(dt), HOUR) as rounded_event_date,
        lower(replace(city_name, ' ', '_')) as city_name,
        cloud_cover, 
        wind_deg, 
        wind_speed, 
        pressure,
        round(temp_k - 273) as temp_c,
        humidity,
        visibility
    FROM {{ ref('raw_weather') }}
) AS subquery



