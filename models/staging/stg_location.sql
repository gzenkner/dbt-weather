SELECT
    {{ dbt_utils.generate_surrogate_key(['city_name']) }} as city_key,
    lower(replace(city_name, ' ', '_')) as city_name,
    base,
    timezone,
    round(lat, 4) as lat,
    round(lon, 4) as lon,
    lower(country) as country
from {{ ref('raw_location') }}