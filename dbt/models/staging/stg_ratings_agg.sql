/*
Staging Model: stg_ratings_agg
Description: Clean daily aggregated ratings

Source: raw.ratings_agg
Grain: One row per listing per date

Transformations:
- Deduplicate (shouldn't have duplicates, but safe)
- Ensure ratings are in valid range (0-5)
*/

with source as (
    select * from {{ source('raw_data', 'ratings_agg') }}
),

deduplicated as (
    select
        date,
        listing_id,
        cnt_ratings as rating_count,
        avg_rating,
        loaded_at,

        row_number() over (
            partition by date, listing_id
            order by loaded_at desc
        ) as rn

    from source
),

cleaned as (
    select
        date,
        listing_id,
        rating_count,

        -- Ensure rating is in valid range
        case
            when avg_rating < 0 then 0
            when avg_rating > 5 then 5
            else avg_rating
        end as avg_rating,

        loaded_at

    from deduplicated
    where rn = 1
      and rating_count > 0  -- Only include dates with actual ratings
)

select * from cleaned
