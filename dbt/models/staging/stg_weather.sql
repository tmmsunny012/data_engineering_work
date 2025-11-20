/*
Staging Model: stg_weather
Description: Aggregate hourly weather to daily averages

Source: raw.weather
Grain: One row per outlet per date (aggregated from hourly)

Transformations:
- Aggregate hourly data to daily averages
- Handle NULL weather values
- Extract date from datetime
*/

with source as (
    select * from {{ source('raw_data', 'weather') }}
),

daily_aggregated as (
    select
        outlet_id,
        cast(datetime as date) as date,

        -- Daily averages
        avg(temperature_2m) as avg_temperature,
        avg(relative_humidity_2m) as avg_humidity,
        avg(wind_speed_10m) as avg_wind_speed,

        -- Additional stats (optional, useful for analysis)
        min(temperature_2m) as min_temperature,
        max(temperature_2m) as max_temperature,

        -- Count of hourly records (should be 24 for complete day)
        count(*) as hourly_records_count

    from source
    where temperature_2m is not null  -- Exclude NULL weather data
    group by
        outlet_id,
        cast(datetime as date)
),

final as (
    select
        outlet_id,
        date,

        -- Round to 2 decimal places for cleaner data
        round(avg_temperature::numeric, 2) as avg_temperature,
        round(avg_humidity::numeric, 2) as avg_humidity,
        round(avg_wind_speed::numeric, 2) as avg_wind_speed,
        round(min_temperature::numeric, 2) as min_temperature,
        round(max_temperature::numeric, 2) as max_temperature,

        hourly_records_count,

        -- Flag if we have complete daily data
        case
            when hourly_records_count = 24 then true
            else false
        end as is_complete_day

    from daily_aggregated
)

select * from final
