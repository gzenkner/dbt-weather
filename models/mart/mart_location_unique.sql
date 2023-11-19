with RankedRows AS (
    SELECT
        name,
        base,
        timezone,
        lat,
        lon,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY 1) AS row_num
    from {{ ref('stg_location_duplicates') }}
)

SELECT
    name,
    base,
    timezone,
    lat,
    lon
FROM RankedRows
WHERE row_num = 1