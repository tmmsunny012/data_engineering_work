-- Database Initialization Script
-- This runs automatically when PostgreSQL container starts for the first time

-- ============================================================================
-- SCHEMAS
-- ============================================================================
-- Organizing our database into logical schemas:
-- 1. raw: Direct CSV loads (no transformations)
-- 2. staging: Cleaned, deduplicated data from raw
-- 3. intermediate: Business logic transformations
-- 4. mart: Final reporting tables for business users

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS mart;

COMMENT ON SCHEMA raw IS 'Raw data from CSV files and APIs - no transformations';
COMMENT ON SCHEMA staging IS 'Cleaned and deduplicated data from raw layer';
COMMENT ON SCHEMA intermediate IS 'Intermediate transformations with business logic';
COMMENT ON SCHEMA mart IS 'Final reporting tables optimized for business analytics';

-- ============================================================================
-- RAW SCHEMA TABLES - Direct CSV mappings
-- ============================================================================

-- Listings: Outlets selling on platforms
CREATE TABLE IF NOT EXISTS raw.listing (
    id INTEGER,
    outlet_id INTEGER,
    platform_id INTEGER,
    timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.listing IS 'Listings of outlets on delivery platforms';

-- Orders: Transaction-level order data
CREATE TABLE IF NOT EXISTS raw.orders (
    listing_id INTEGER,
    order_id INTEGER,
    placed_at TIMESTAMP,
    status VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.orders IS 'Individual order transactions';

-- Orders Daily: Pre-aggregated daily order counts
CREATE TABLE IF NOT EXISTS raw.orders_daily (
    date DATE,
    listing_id INTEGER,
    orders INTEGER,
    timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.orders_daily IS 'Daily aggregated order counts per listing';

-- Organizations: Parent companies owning outlets
CREATE TABLE IF NOT EXISTS raw.org (
    id INTEGER,
    name VARCHAR(255),
    timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.org IS 'Organizations (parent companies) owning outlets';

-- Outlets: Physical restaurant locations
CREATE TABLE IF NOT EXISTS raw.outlet (
    id INTEGER,
    org_id INTEGER,
    name VARCHAR(255),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.outlet IS 'Physical outlet locations with coordinates for weather data';

-- Platforms: Delivery platforms (UberEats, DoorDash, etc.)
CREATE TABLE IF NOT EXISTS raw.platform (
    id INTEGER,
    "group" VARCHAR(100),
    name VARCHAR(100),
    country VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.platform IS 'Delivery and aggregator platforms';

-- Rank: Listing visibility rankings over time
CREATE TABLE IF NOT EXISTS raw.rank (
    listing_id INTEGER,
    date DATE,
    timestamp TIMESTAMP,
    is_online BOOLEAN,
    rank DECIMAL(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.rank IS 'Listing rankings throughout the day (multiple readings per day)';

-- Ratings: Aggregated daily ratings
CREATE TABLE IF NOT EXISTS raw.ratings_agg (
    date DATE,
    listing_id INTEGER,
    cnt_ratings INTEGER,
    avg_rating DECIMAL(3, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.ratings_agg IS 'Daily aggregated ratings per listing';

-- Weather: Hourly weather data from Open-Meteo API
CREATE TABLE IF NOT EXISTS raw.weather (
    outlet_id INTEGER,
    datetime TIMESTAMP,
    temperature_2m DECIMAL(5, 2),
    relative_humidity_2m INTEGER,
    wind_speed_10m DECIMAL(5, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE raw.weather IS 'Hourly weather data fetched from Open-Meteo API';

-- ============================================================================
-- INDEXES for better query performance
-- ============================================================================

-- Raw table indexes (helps with joins in DBT)
CREATE INDEX IF NOT EXISTS idx_raw_listing_id ON raw.listing(id);
CREATE INDEX IF NOT EXISTS idx_raw_listing_outlet ON raw.listing(outlet_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_listing ON raw.orders(listing_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_daily_listing_date ON raw.orders_daily(listing_id, date);
CREATE INDEX IF NOT EXISTS idx_raw_outlet_id ON raw.outlet(id);
CREATE INDEX IF NOT EXISTS idx_raw_platform_id ON raw.platform(id);
CREATE INDEX IF NOT EXISTS idx_raw_rank_listing_date ON raw.rank(listing_id, date);
CREATE INDEX IF NOT EXISTS idx_raw_ratings_listing_date ON raw.ratings_agg(listing_id, date);
CREATE INDEX IF NOT EXISTS idx_raw_weather_outlet_datetime ON raw.weather(outlet_id, datetime);

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Grant usage on schemas
GRANT USAGE ON SCHEMA raw TO dataeng;
GRANT USAGE ON SCHEMA staging TO dataeng;
GRANT USAGE ON SCHEMA intermediate TO dataeng;
GRANT USAGE ON SCHEMA mart TO dataeng;

-- Grant all privileges on all tables in schemas
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA intermediate TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA mart TO dataeng;

-- Grant privileges on future tables (for DBT)
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT ALL ON TABLES TO dataeng;

-- ============================================================================
-- SETUP COMPLETE
-- ============================================================================

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully!';
    RAISE NOTICE 'Schemas created: raw, staging, intermediate, mart';
    RAISE NOTICE 'Ready for data loading and DBT transformations';
END $$;
