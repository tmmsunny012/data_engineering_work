/*
Mart Model: fct_daily_business_performance
Description: Daily business performance metrics combining all data sources

Grain: One row per listing per date

This is the FINAL REPORTING TABLE used by business users to:
- Analyze daily sales performance
- Correlate weather with order volume
- Track rating trends and impact on orders
- Monitor platform rankings
- Compare outlet and organization performance

Business Questions Answered:
1. How does weather affect order volume?
2. Do better ratings lead to more orders?
3. Does higher ranking correlate with sales?
4. Which outlets/orgs perform best?
5. Which platforms drive most revenue?
*/

with orders_daily as (
    select * from {{ ref('stg_orders_daily') }}
),

listings as (
    select * from {{ ref('stg_listing') }}
),

outlets as (
    select * from {{ ref('stg_outlet') }}
),

orgs as (
    select * from {{ ref('stg_org') }}
),

platforms as (
    select * from {{ ref('stg_platform') }}
),

ratings_metrics as (
    select * from {{ ref('int_ratings_metrics') }}
),

rank_daily as (
    select * from {{ ref('int_rank_daily_avg') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

-- Order status breakdown from individual order records
orders_by_status as (
    select
        order_date as date,
        listing_id,

        -- Total orders (count of individual order records)
        count(*) as total_orders,

        -- By status
        count(*) filter (where status = 'completed') as completed_orders,
        count(*) filter (where status = 'pending') as pending_orders,
        count(*) filter (where status = 'cancelled') as cancelled_orders,
        count(*) filter (where status = 'refunded') as refunded_orders,

        -- Business logic categories
        count(*) filter (where status in ('completed', 'pending')) as successful_orders,
        count(*) filter (where status in ('cancelled', 'refunded')) as failed_orders,

        -- Failure rate
        round(
            100.0 * count(*) filter (where status in ('cancelled', 'refunded')) / count(*),
            1
        ) as order_failure_rate

    from {{ ref('stg_orders') }}
    group by order_date, listing_id
),

-- Generate date spine for all listing-date combinations
-- This ensures we have a row even if no orders on a particular day
date_spine as (
    select distinct
        date
    from orders_daily
),

listing_spine as (
    select distinct
        listing_id
    from listings
),

all_combinations as (
    select
        d.date,
        l.listing_id
    from date_spine d
    cross join listing_spine l
),

-- Join everything together
final as (
    select
        -- Date dimension
        ac.date,

        -- Listing dimension
        ac.listing_id,
        l.listing_timestamp,

        -- Outlet dimension
        l.outlet_id,
        out.outlet_name,
        out.latitude,
        out.longitude,
        out.has_valid_coordinates,

        -- Organization dimension
        out.org_id,
        org.org_name,

        -- Platform dimension
        l.platform_id,
        p.platform_name,
        p.platform_group,
        p.platform_country,

        -- Order metrics (dual sources for data quality comparison)
        coalesce(od.daily_orders, 0) as daily_orders_from_snapshot,  -- From orders_daily.csv (has quality issues)
        coalesce(obs.total_orders, 0) as daily_orders,  -- From orders.csv (PRIMARY SOURCE OF TRUTH)
        coalesce(obs.total_orders, 0) as total_orders,  -- Kept for backward compatibility

        -- Data quality indicators
        case
            when od.daily_orders is null then 'snapshot_missing'
            when od.daily_orders < 0 then 'snapshot_negative'
            when abs(coalesce(od.daily_orders, 0) - coalesce(obs.total_orders, 0)) > 5 then 'large_discrepancy'
            when od.daily_orders != obs.total_orders then 'minor_discrepancy'
            else 'match'
        end as data_quality_flag,

        (coalesce(obs.total_orders, 0) - coalesce(od.daily_orders, 0)) as snapshot_vs_actual_diff,

        -- Order status breakdown (from orders.csv only)
        coalesce(obs.completed_orders, 0) as completed_orders,
        coalesce(obs.pending_orders, 0) as pending_orders,
        coalesce(obs.cancelled_orders, 0) as cancelled_orders,
        coalesce(obs.refunded_orders, 0) as refunded_orders,

        -- Business logic categories
        coalesce(obs.successful_orders, 0) as successful_orders,
        coalesce(obs.failed_orders, 0) as failed_orders,
        coalesce(obs.order_failure_rate, 0) as order_failure_rate,

        -- Rating metrics
        coalesce(rm.daily_new_ratings, 0) as daily_new_ratings,
        coalesce(rm.cumulative_rating_count, 0) as cumulative_rating_count,
        coalesce(rm.avg_rating, 0) as avg_rating,
        coalesce(rm.rating_delta, 0) as rating_delta,
        coalesce(rm.rating_trend, 'No Ratings') as rating_trend,

        -- Rank metrics
        rd.avg_rank,
        rd.best_rank as daily_best_rank,
        rd.worst_rank as daily_worst_rank,
        rd.rank_volatility,

        -- Uptime/Availability metrics
        coalesce(rd.total_readings, 0) as rank_total_readings,
        coalesce(rd.online_readings, 0) as rank_online_readings,
        coalesce(rd.offline_readings, 0) as rank_offline_readings,
        coalesce(rd.uptime_percentage, 0) as uptime_percentage,
        coalesce(rd.was_ever_online, false) as was_ever_online,
        coalesce(rd.availability_category, 'No Data') as availability_category,

        -- Weather metrics (from outlet location)
        w.avg_temperature,
        w.avg_humidity,
        w.avg_wind_speed,
        w.min_temperature,
        w.max_temperature,
        coalesce(w.is_complete_day, false) as weather_data_complete,

        -- Derived metrics
        case
            when coalesce(rm.avg_rating, 0) >= 4.5 then 'Excellent'
            when coalesce(rm.avg_rating, 0) >= 4.0 then 'Good'
            when coalesce(rm.avg_rating, 0) >= 3.0 then 'Average'
            when coalesce(rm.avg_rating, 0) > 0 then 'Poor'
            else 'No Ratings'
        end as rating_category,

        case
            when w.avg_temperature < 10 then 'Cold'
            when w.avg_temperature < 20 then 'Mild'
            when w.avg_temperature < 30 then 'Warm'
            when w.avg_temperature is not null then 'Hot'
            else 'Unknown'
        end as temperature_category,

        -- Data quality anomaly flags
        case
            when (coalesce(od.daily_orders, 0) > 0 or coalesce(obs.total_orders, 0) > 0)
                 and coalesce(rd.uptime_percentage, 0) = 0
            then true
            else false
        end as is_offline_order_anomaly,

        -- Offline-related failure flag
        case
            when coalesce(rd.uptime_percentage, 0) > 0
                 and coalesce(rd.uptime_percentage, 0) < 50
                 and coalesce(obs.order_failure_rate, 0) > 30
            then true
            else false
        end as is_high_failure_when_offline

    from all_combinations ac

    -- Join with listing (to get outlet, platform)
    left join listings l
        on ac.listing_id = l.listing_id

    -- Join with outlet
    left join outlets out
        on l.outlet_id = out.outlet_id

    -- Join with org
    left join orgs org
        on out.org_id = org.org_id

    -- Join with platform
    left join platforms p
        on l.platform_id = p.platform_id

    -- Join with orders daily
    left join orders_daily od
        on ac.date = od.date
        and ac.listing_id = od.listing_id

    -- Join with order status breakdown
    left join orders_by_status obs
        on ac.date = obs.date
        and ac.listing_id = obs.listing_id

    -- Join with ratings
    left join ratings_metrics rm
        on ac.date = rm.date
        and ac.listing_id = rm.listing_id

    -- Join with rank
    left join rank_daily rd
        on ac.date = rd.date
        and ac.listing_id = rd.listing_id

    -- Join with weather (by outlet, not listing)
    left join weather w
        on ac.date = w.date
        and l.outlet_id = w.outlet_id

    -- Filter to only dates where we have data
    where od.daily_orders is not null
       or obs.total_orders is not null
       or rm.avg_rating is not null
       or rd.avg_rank is not null
)

select * from final
