/*
Staging Model: stg_orders
Description: Clean order transaction data

Source: raw.orders
Grain: One row per order (deduplicated by order_id)

Transformations:
- Deduplicate by order_id (keep latest)
- Standardize status values
- Extract date from timestamp
*/

with source as (
    select * from {{ source('raw_data', 'orders') }}
),

deduplicated as (
    select
        order_id,
        listing_id,
        placed_at,
        status,
        loaded_at,

        -- Deduplication
        row_number() over (
            partition by order_id
            order by placed_at desc
        ) as rn

    from source
),

cleaned as (
    select
        order_id,
        listing_id,
        placed_at,
        cast(placed_at as date) as order_date,

        -- Standardize status (lowercase)
        lower(trim(status)) as status,

        loaded_at

    from deduplicated
    where rn = 1
)

select * from cleaned
