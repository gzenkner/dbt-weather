SELECT
    dt,
    timestamp_seconds(dt) as date,
    city_name,
    lat,
    lon,
    (temp - 273) as temp,
    (dew_point - 273) as dew_point,
    pressure,
    humidity,
    wind_speed,
    wind_deg,
    wind_gust,
    COALESCE(rain_1h, 0) as rain_1h,
    COALESCE(clouds_all, 0) as clouds_all,
    visibility
from {{ ref('stg_weather_training') }}