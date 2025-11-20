/*
Staging Model: stg_org
Description: Clean organization data

Source: raw.org
Grain: One row per organization

Transformations:
- Deduplicate by org_id (keep latest)
- Trim organization names
*/

with source as (
    select * from {{ source('raw_data', 'org') }}
),

deduplicated as (
    select
        id as org_id,
        name as org_name,
        timestamp as org_timestamp,
        loaded_at,

        row_number() over (
            partition by id
            order by timestamp desc
        ) as rn

    from source
),

final as (
    select
        org_id,
        trim(org_name) as org_name,
        org_timestamp,
        loaded_at

    from deduplicated
    where rn = 1
)

select * from final
