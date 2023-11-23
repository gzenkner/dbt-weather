with source as (

    select * from {{ source('weather_api', 'weather_api_table_raw') }}

),

renamed as (

    select
        CAST(JSON_EXTRACT(string, '$.dt') AS INT64) AS dt,
        REGEXP_REPLACE(JSON_EXTRACT(string, '$.name'), r'"', '') AS city_name,
        CAST(JSON_EXTRACT(string, '$.clouds.all') AS FLOAT64) AS cloud_cover,
        CAST(JSON_EXTRACT(string, '$.wind.deg') AS FLOAT64) AS wind_deg,
        CAST(JSON_EXTRACT(string, '$.wind.speed') AS FLOAT64) AS wind_speed,
        CAST(JSON_EXTRACT(string, '$.main.pressure') AS FLOAT64) AS pressure,
        CAST(JSON_EXTRACT(string, '$.main.temp') AS FLOAT64) AS temp_k,
        CAST(JSON_EXTRACT(string, '$.main.humidity') AS FLOAT64) AS humidity,
        CAST(JSON_EXTRACT(string, '$.visibility') AS FLOAT64) AS visibility

    from source


)

select * from renamed
