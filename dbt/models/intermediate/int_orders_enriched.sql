/*
Intermediate Model: int_orders_enriched
Description: Orders joined with all dimensional data

Grain: One row per order

Transformations:
- Join orders with listing, outlet, org, platform
- Add all contextual information
- Calculate order-level metrics
*/

with orders as (
    select * from {{ ref('stg_orders') }}
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

enriched as (
    select
        -- Order details
        o.order_id,
        o.order_date,
        o.placed_at,
        o.status,

        -- Listing details
        o.listing_id,
        l.listing_timestamp,

        -- Outlet details
        l.outlet_id,
        out.outlet_name,
        out.latitude,
        out.longitude,
        out.has_valid_coordinates,

        -- Organization details
        out.org_id,
        org.org_name,

        -- Platform details
        l.platform_id,
        p.platform_name,
        p.platform_group,
        p.platform_country

    from orders o
    inner join listings l
        on o.listing_id = l.listing_id
    inner join outlets out
        on l.outlet_id = out.outlet_id
    inner join orgs org
        on out.org_id = org.org_id
    inner join platforms p
        on l.platform_id = p.platform_id
)

select * from enriched
