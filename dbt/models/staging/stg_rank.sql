/*
Staging Model: stg_rank
Description: Clean ranking data (multiple readings per day)

Source: raw.rank
Grain: Multiple rows per listing per date (hourly snapshots)

Transformations:
- Keep ALL records (online AND offline) to track uptime
- Preserve is_online flag for downstream uptime calculations
- Note: Rank is NULL when is_online = false (expected)

Data Quality Note:
- We keep offline readings to detect anomalies (orders while offline)
- Downstream models calculate uptime percentage
*/

with source as (
    select * from {{ source('raw_data', 'rank') }}
),

cleaned as (
    select
        listing_id,
        date,
        timestamp as rank_timestamp,
        is_online,
        rank,
        loaded_at

    from source
    -- Keep ALL records (no filter on is_online)
    -- Offline records have rank = NULL (expected)
)

select * from cleaned
