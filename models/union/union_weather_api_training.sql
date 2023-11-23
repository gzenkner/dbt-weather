with sources as (
    select * from {{ ref('dedupe_weather_training') }}
    union all
    select * from {{ ref('dedupe_weather') }}
)

select *
from sources
