with sources as (
    select * from {{ source('weather_training', 'weather_city_of_london_training') }}
    union all
    select * from {{ source('weather_training', 'weather_cockfosters_historical') }}
)

select *
from sources

