/*
Staging Model: stg_platform
Description: Clean platform data

Source: raw.platform
Grain: One row per platform

Transformations:
- No deduplication needed (no timestamp column)
- Standardize naming
*/

with source as (
    select * from {{ source('raw_data', 'platform') }}
),

final as (
    select
        id as platform_id,
        "group" as platform_group,  -- "group" is reserved keyword, need quotes
        name as platform_name,
        country as platform_country,
        loaded_at

    from source
)

select * from final
