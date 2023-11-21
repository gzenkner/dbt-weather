with RankedRows AS (
    SELECT
        city_name,
        base,
        timezone,
        lat,
        lon,
        country,
        ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY 1) AS row_num
    from {{ ref('stg_location_duplicates') }}
)

SELECT
    lower(replace(city_name, ' ', '_')) as city_name,
    base,
    timezone,
    lat,
    lon,
    country
FROM RankedRows
WHERE row_num = 1