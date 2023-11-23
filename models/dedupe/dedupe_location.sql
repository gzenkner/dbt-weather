with RankedRows AS (
    select
        city_key,
        city_name,
        base,
        timezone,
        lat,
        lon,
        country,
        ROW_NUMBER() OVER (partition by city_name order by 1) as row_num
    from {{ ref('stg_location') }}
)

select
    city_key,
    city_name,
    base,
    timezone,
    lat,
    lon,
    country
from RankedRows
where row_num = 1