/*
Staging Model: stg_orders_daily
Description: Daily aggregated order counts

Source: raw.orders_daily
Grain: One row per listing per date

Transformations:
- Deduplicate by date + listing_id (keep latest timestamp)
- Ensure orders count is non-negative
*/

with source as (
    select * from {{ source('raw_data', 'orders_daily') }}
),

deduplicated as (
    select
        date,
        listing_id,
        orders,
        timestamp,
        loaded_at,

        -- Keep latest record per date + listing
        row_number() over (
            partition by date, listing_id
            order by timestamp desc
        ) as rn

    from source
),

final as (
    select
        date,
        listing_id,

        -- Ensure orders is non-negative
        case
            when orders < 0 then 0
            else orders
        end as daily_orders,

        timestamp,
        loaded_at

    from deduplicated
    where rn = 1
)

select * from final
