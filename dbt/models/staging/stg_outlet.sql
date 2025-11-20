/*
Staging Model: stg_outlet
Description: Clean outlet location data

Source: raw.outlet
Grain: One row per outlet

Transformations:
- Deduplicate by outlet_id (keep latest)
- Flag invalid coordinates (0, 0)
- Standardize naming
*/

with source as (
    select * from {{ source('raw_data', 'outlet') }}
),

deduplicated as (
    select
        id as outlet_id,
        org_id,
        name as outlet_name,
        latitude,
        longitude,
        timestamp as outlet_timestamp,
        loaded_at,

        row_number() over (
            partition by id
            order by timestamp desc
        ) as rn

    from source
),

final as (
    select
        outlet_id,
        org_id,
        outlet_name,
        latitude,
        longitude,

        -- Flag if coordinates are valid
        case
            when latitude = 0 and longitude = 0 then false
            when latitude is null or longitude is null then false
            else true
        end as has_valid_coordinates,

        outlet_timestamp,
        loaded_at

    from deduplicated
    where rn = 1
)

select * from final
