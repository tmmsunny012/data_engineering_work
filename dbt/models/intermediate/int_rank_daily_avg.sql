/*
Intermediate Model: int_rank_daily_avg
Description: Calculate average daily rank AND uptime metrics per listing

Grain: One row per listing per date

Transformations:
- Aggregate multiple daily rank readings to average (online readings only)
- Calculate rank statistics (min, max)
- Calculate uptime percentage (online vs offline readings)
- Flag days where listing was completely offline
*/

with ranks as (
    select * from {{ ref('stg_rank') }}
),

daily_aggregated as (
    select
        listing_id,
        date,

        -- Rank metrics (only from online readings, 0 = offline all day)
        round(coalesce(avg(rank) filter (where is_online = true), 0)::numeric, 2) as avg_rank,
        coalesce(min(rank) filter (where is_online = true), 0) as best_rank,
        coalesce(max(rank) filter (where is_online = true), 0) as worst_rank,

        -- Uptime/availability metrics
        count(*) as total_readings,
        count(*) filter (where is_online = true) as online_readings,
        count(*) filter (where is_online = false) as offline_readings,

        -- Uptime percentage (0-100)
        round(
            100.0 * count(*) filter (where is_online = true) / count(*),
            1
        ) as uptime_percentage,

        -- Was the listing online at any point during the day?
        bool_or(is_online) as was_ever_online

    from ranks
    group by
        listing_id,
        date
),

final as (
    select
        listing_id,
        date,

        -- Rank metrics
        avg_rank,
        best_rank,
        worst_rank,

        -- Rank volatility (range of ranks)
        round((worst_rank - best_rank)::numeric, 2) as rank_volatility,

        -- Uptime/availability metrics
        total_readings,
        online_readings,
        offline_readings,
        uptime_percentage,
        was_ever_online,

        -- Categorize availability
        case
            when uptime_percentage = 100 then 'Always Online'
            when uptime_percentage >= 75 then 'Mostly Online'
            when uptime_percentage >= 25 then 'Partially Online'
            when uptime_percentage > 0 then 'Mostly Offline'
            else 'Always Offline'
        end as availability_category

    from daily_aggregated
)

select * from final
