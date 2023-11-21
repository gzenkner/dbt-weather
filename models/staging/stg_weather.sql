with source as (

    select * from {{ source('weather_api', 'weather_api_table_raw') }}

),

renamed as (

    select
        CAST(JSON_EXTRACT(string, '$.dt') AS INT64) AS dt,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.name'), r'"', '') AS city_name,
        CAST(JSON_EXTRACT(string, '$.coord.lon') AS FLOAT64) AS lon,
        CAST(JSON_EXTRACT(string, '$.coord.lat') AS FLOAT64) AS lat,
        CAST(JSON_EXTRACT(string, '$.main.temp') AS FLOAT64) AS temp_k,
        CAST(JSON_EXTRACT(string, '$.main.pressure') AS FLOAT64) AS pressure,
        CAST(JSON_EXTRACT(string, '$.main.humidity') AS FLOAT64) AS humidity,
        CAST(JSON_EXTRACT(string, '$.wind.speed') AS FLOAT64) AS speed,
        CAST(JSON_EXTRACT(string, '$.wind.deg') AS FLOAT64) AS deg,
        CAST(JSON_EXTRACT(string, '$.clouds.all') AS FLOAT64) AS cloud_cover,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.base'), r'"', '') AS base,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.sys.country'), r'"', '') AS country,
        CAST(JSON_EXTRACT(string, '$.timezone') AS FLOAT64) AS timezone,
        CAST(JSON_EXTRACT(string, '$.visibility') AS FLOAT64) AS visibility

    from source


)

select * from renamed
