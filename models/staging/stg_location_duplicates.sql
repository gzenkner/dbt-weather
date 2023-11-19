with source as (

    select * from {{ source('weather_api', 'weather_api_table') }}

),

renamed as (

    select
        name,
        base,
        timezone,
        coord.lat AS lat,
        coord.lon AS lon

    from source
    GROUP BY name, base, timezone, lat, lon


)

select * from renamed
