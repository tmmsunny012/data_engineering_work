/*
Staging Model: stg_listing
Description: Deduplicated listing data

Source: raw.listing
Grain: One row per listing (deduplicated by latest timestamp)

Transformations:
- Remove duplicates by keeping latest timestamp
- Rename columns to standard naming
- Ensure data types are correct
*/

with source as (
    select * from {{ source('raw_data', 'listing') }}
),

deduplicated as (
    select
        id as listing_id,
        outlet_id,
        platform_id,
        timestamp as listing_timestamp,

        -- Add metadata
        loaded_at,

        -- Row number for deduplication (keep latest timestamp)
        row_number() over (
            partition by id
            order by timestamp desc
        ) as rn

    from source
),

final as (
    select
        listing_id,
        outlet_id,
        platform_id,
        listing_timestamp,
        loaded_at

    from deduplicated
    where rn = 1  -- Keep only the latest record per listing
)

select * from final
