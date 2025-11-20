/*
Intermediate Model: int_ratings_metrics
Description: Calculate daily and cumulative rating metrics

Grain: One row per listing per date

IMPORTANT: Source data (cnt_ratings) is ALREADY CUMULATIVE!
- cnt_ratings = total ratings received up to this date
- We need to calculate daily NEW ratings by taking the difference

Transformations:
- Calculate daily NEW rating count (delta from previous day)
- rating_count from source is already cumulative (use as-is)
- Calculate rating delta from previous day
- Add trend indicators
- Default to 0 for metrics when no data available
*/

with ratings as (
    select * from {{ ref('stg_ratings_agg') }}
),

with_metrics as (
    select
        date,
        listing_id,
        rating_count,  -- This is CUMULATIVE from source
        avg_rating,

        -- Previous day's cumulative count
        lag(rating_count) over (
            partition by listing_id
            order by date
        ) as prev_cumulative_count,

        -- Previous day's rating for delta calculation
        lag(avg_rating) over (
            partition by listing_id
            order by date
        ) as prev_day_rating

    from ratings
),

final as (
    select
        date,
        listing_id,

        -- DAILY new ratings (calculated from cumulative)
        case
            when prev_cumulative_count is null then rating_count  -- First day, all ratings are new
            else rating_count - prev_cumulative_count  -- Daily new = current cumulative - previous cumulative
        end as daily_new_ratings,

        -- CUMULATIVE total ratings (from source, default to 0)
        coalesce(rating_count, 0) as cumulative_rating_count,

        -- Average rating (default to 0)
        coalesce(avg_rating, 0) as avg_rating,

        -- Calculate rating delta (change from previous day, default to 0)
        coalesce(
            case
                when prev_day_rating is null then null  -- First day, no comparison
                else round((avg_rating - prev_day_rating)::numeric, 2)
            end,
            0
        ) as rating_delta,

        -- Rating trend indicator (default to 'new')
        case
            when prev_day_rating is null then 'new'
            when avg_rating > prev_day_rating then 'improving'
            when avg_rating < prev_day_rating then 'declining'
            else 'stable'
        end as rating_trend

    from with_metrics
)

select * from final
