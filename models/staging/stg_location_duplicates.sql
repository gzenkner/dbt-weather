with source as (

    select * from {{ source('weather_api', 'weather_api_table_raw') }}

),

renamed as (

    select
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.name'), r'"', '') AS city_name,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.base'), r'"', '') AS base,
        CAST(JSON_EXTRACT(string, '$.timezone') AS FLOAT64) AS timezone,
        CAST(JSON_EXTRACT(string, '$.coord.lat') AS FLOAT64) AS lat,
        CAST(JSON_EXTRACT(string, '$.coord.lon') AS FLOAT64) AS lon,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.sys.country'), r'"', '') AS country
    
    from source
    GROUP BY city_name, base, timezone, lat, lon, country


)

select * from renamed
