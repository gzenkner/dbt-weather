with source as (

    select * from {{ source('weather_api', 'weather_api_table') }}

),

renamed as (

    select
        dt,
        name,
        clouds.all as clouds,
        rain.1h as rain_per_hour,
        wind.gust as wind_gust,
        wind.deg as wind_deg,
        wind.speed as wind_speed, 
        main.pressure as pressure,
        main.temp as temp,
        main.humidity as humidity,
        visibility

    from source


)

select * from renamed
