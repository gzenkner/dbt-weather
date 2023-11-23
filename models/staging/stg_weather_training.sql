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
        {{ dbt_utils.generate_surrogate_key(['DATETIME_TRUNC(TIMESTAMP_SECONDS(dt), HOUR)', 'raw_weather_training.city_name']) }} as location_time_key,
        dt,
        TIMESTAMP_SECONDS(dt) as event_date,
        DATETIME_TRUNC(TIMESTAMP_SECONDS(dt), HOUR) as rounded_event_date,
        case
            when city_name = 'Custom location' then 'clerkenwell'
            when city_name = 'Cockfosters' then 'hadley_wood'
        end as city_name,
        cast(clouds_all as FLOAT64) as cloud_cover, 
        cast(wind_deg as FLOAT64) as wind_deg, 
        cast(wind_speed as FLOAT64) as wind_speed,
        cast(pressure as FLOAT64) as pressure, 
        cast(round(temp - 273) as FLOAT64) as temp_c, 
        cast(humidity as FLOAT64) as humidity, 
        cast(visibility as FLOAT64) as visibility  
    FROM {{ ref('raw_weather_training') }}
) AS subquery
