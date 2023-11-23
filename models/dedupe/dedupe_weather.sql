WITH RankedRows AS (
  select
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
    visibility,
    ROW_NUMBER() OVER (partition by location_time_key order by dt) as row_num
  from {{ ref('stg_weather') }}
)

select
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
from RankedRows
where row_num = 1