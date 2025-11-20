{% docs __overview__ %}

# Business Performance Analytics - DBT Documentation

## 📊 Project Overview

This DBT project transforms raw business and weather data into actionable insights for restaurant delivery platform analytics.

### Business Goals

1. **Understand weather impact on orders** - How do temperature, humidity, and wind affect customer behavior?
2. **Analyze rating trends** - Do better ratings lead to more orders?
3. **Monitor platform performance** - Which platforms drive the most business?
4. **Track ranking correlation** - Does higher visibility translate to more sales?
5. **Benchmark outlets and organizations** - Who performs best and why?

---

## 🏗️ Architecture

### Data Flow

```
raw schema (source data)
    ↓
staging schema (cleaned, deduplicated)
    ↓
intermediate schema (business logic)
    ↓
mart schema (final reporting)
```

### Schema Layers

#### **Raw Layer**
- Direct CSV loads and API data
- No transformations
- Includes duplicates and data quality issues
- Source of truth

#### **Staging Layer**
- One model per raw table
- Deduplication (keep latest timestamp)
- Data type standardization
- Column renaming
- Basic data quality filters

**Models:**
- `stg_listing` - Deduplicated listings
- `stg_orders` - Clean order transactions
- `stg_orders_daily` - Daily order aggregates
- `stg_outlet` - Outlet locations with coordinate validation
- `stg_org` - Organizations
- `stg_platform` - Delivery platforms
- `stg_rank` - Ranking snapshots (online only)
- `stg_ratings_agg` - Daily ratings
- `stg_weather` - Hourly weather aggregated to daily

#### **Intermediate Layer**
- Business logic transformations
- Complex calculations
- Reusable building blocks

**Models:**
- `int_orders_enriched` - Orders with all dimensional data
- `int_ratings_metrics` - Cumulative ratings + delta calculations
- `int_rank_daily_avg` - Average daily ranks with volatility

#### **Mart Layer**
- Business-ready tables
- Optimized for queries
- Used by dashboards and analysts

**Models:**
- `fct_daily_business_performance` - **THE MAIN TABLE**

---

## 📈 Key Model: fct_daily_business_performance

### Grain
**One row per listing per date**

### Dimensions
- **Date** - Daily grain (2023-01-01 to 2023-12-31)
- **Listing** - Outlet on platform
- **Outlet** - Physical restaurant location
- **Organization** - Parent company
- **Platform** - Delivery/aggregator platform

### Metrics

| Category | Metrics | Description |
|----------|---------|-------------|
| **Orders** | daily_orders, completed_orders, failed_orders, pending_orders, cancelled_orders | Order volume and status breakdown |
| **Order Quality** | order_failure_rate, successful_orders, data_quality_flag | Success/failure tracking and data validation |
| **Ratings** | avg_rating, cumulative_rating_count, rating_delta, rating_trend | Rating performance |
| **Rank** | avg_rank, daily_best_rank, daily_worst_rank, rank_volatility | Platform visibility |
| **Availability** | uptime_percentage, availability_category, rank_total_readings | Online presence tracking |
| **Weather** | avg_temperature, avg_humidity, avg_wind_speed, min_temperature, max_temperature | Weather conditions |

### Data Quality Features

The fact table includes built-in data quality monitoring by comparing two data sources:

- **Primary Source:** `daily_orders` from orders.csv (individual transaction records) - **USE THIS FOR ALL ANALYSIS**
- **Secondary Source:** `daily_orders_from_snapshot` from orders_daily.csv (pre-aggregated) - **DEPRECATED - quality issues identified**
- **Quality Flags:** Automatic categorization of data quality issues (99.98% match rate)
- **Transparency:** Both sources visible for validation and auditing

### Sample Queries

**Daily performance overview:**
```sql
SELECT
    date,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as avg_rating,
    AVG(avg_temperature) as avg_temp
FROM mart.fct_daily_business_performance
WHERE date >= '2023-01-01'
GROUP BY date
ORDER BY date;
```

**Weather correlation:**
```sql
SELECT
    temperature_category,
    AVG(daily_orders) as avg_orders
FROM mart.fct_daily_business_performance
GROUP BY temperature_category
ORDER BY avg_orders DESC;
```

**Top performing outlets:**
```sql
SELECT
    outlet_name,
    org_name,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as avg_rating
FROM mart.fct_daily_business_performance
GROUP BY outlet_name, org_name
ORDER BY total_orders DESC
LIMIT 10;
```

---

## 🧪 Data Quality Tests

### Built-in Tests
- **Uniqueness** - Primary keys are unique
- **Not Null** - Required fields populated
- **Relationships** - Foreign keys reference valid records
- **Accepted Values** - Categorical fields have valid values

### Custom Tests
- **No future dates** - All dates are historical
- **Rating range** - Ratings between 0-5
- **Temperature range** - Temperatures reasonable (-50°C to 60°C)

### Running Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select fct_daily_business_performance

# Run only custom tests
dbt test --select test_type:generic
```

---

## 🎯 Business Use Cases

### Key Business Insights (Based on 2-Year Analysis)

**Business Context:**
- **Total Orders:** 95,982 orders across 200 listings (2023-2024)
- **Success Rate:** 57.1% completed (54,809 orders)
- **Failure Rate:** 14.0% average (1 in 7 orders fails)
- **Operating Coverage:** 5 platforms in 2 countries (UK and US)

### Use Case 1: Weather Impact Analysis
**Question:** Does weather affect order volume?

**Finding:** ❌ **Weather has ZERO impact on order volume**
- All temperature categories (Cold/Mild/Warm) show identical performance: 1.6 orders/day
- Correlation coefficient: -0.003 (essentially no relationship)
- Business operates consistently regardless of weather conditions

**Implication:** Don't invest in weather-based marketing or operational adjustments

### Use Case 2: Rating Impact
**Question:** Do better ratings lead to more sales?

**Finding:** ⚠️ **Unrated listings get 3x MORE orders but fail 3x more often**
- Listings without ratings: 2.2 orders/day, 20.3% failure rate (80.6% of total business)
- Rated listings (Good 4.0-4.5): 0.7 orders/day, 6.0% failure rate
- Correlation between ratings and orders: 0.015 (near zero)

**Implication:** Business prioritizes volume over quality. Converting unrated listings to rated performance levels = major opportunity for reducing failures.

### Use Case 3: Platform Performance
**Question:** Which platforms drive most business?

**Finding:** ✅ **UK platforms lead, very consistent performance across all platforms**
- JustEat (UK): 21,608 orders, 13.4% failure rate (best)
- DoorDash (US): 21,100 orders, 13.4% failure rate (tied best)
- Deliveroo (UK): 19,732 orders, 14.1% failure rate
- GrubHub (US): 19,231 orders, 14.6% failure rate
- UberEats (US): 14,311 orders, 14.8% failure rate (highest failures)

**Implication:** All platforms perform within 1.4% failure rate range. No need for platform-specific strategies.

### Use Case 4: Ranking Effectiveness
**Question:** Does ranking matter?

**Finding:** ⚠️ **Rank data covers only 5.7% of operations**
- 94.3% of orders (90,517) have no rank/uptime data
- Correlation between rank and orders: 0.078 (very weak)
- Top performers average ranks 40-65 (mid-tier visibility)

**Implication:** Current rank tracking insufficient for analysis. Most business operates without visibility data.

### Use Case 5: Operational Excellence
**Question:** What drives success in this business?

**Finding:** ✅ **Execution quality is everything**
- Stable operations: 1.52-1.57 orders/day across all 24 months (no seasonality)
- No day-of-week effects: All days perform identically (2.6% variance)
- Top 5 organizations control 67.3% of market (16,407 orders for leader)
- 10 worst performers have 32-35% failure rates (2.3x average)

**Implication:** Success is driven by operational execution, not external factors. Focus on reducing failure rates for bottom performers.

---

## 📝 Model Dependencies

```
stg_listing ──┐
              ├──> int_orders_enriched ──┐
stg_orders ───┘                          │
                                         │
stg_ratings_agg ──> int_ratings_metrics ─┤
                                         │
stg_rank ──────────> int_rank_daily_avg ─┤
                                         │
                                         ├──> fct_daily_business_performance
stg_outlet ──────────────────────────────┤
stg_org ─────────────────────────────────┤
stg_platform ────────────────────────────┤
stg_weather ─────────────────────────────┘
```

---

## 🚀 Getting Started

### Setup
```bash
# Install DBT
pip install dbt-postgres

# Test connection
cd dbt
dbt debug

# Install dependencies (if any)
dbt deps
```

### Development Workflow
```bash
# 1. Run models
dbt run

# 2. Run tests
dbt test

# 3. Generate docs
dbt docs generate

# 4. View docs
dbt docs serve
```

### Production Workflow
```bash
# Full refresh (rebuild all tables)
dbt run --full-refresh

# Run specific model and downstream dependencies
dbt run --select fct_daily_business_performance+

# Test and fail on error
dbt test --store-failures
```

---

## 📊 Key Performance Indicators Summary

### What Matters
- **Order Failure Rate:** Current average 14.0% (target: <10%)
- **Rated vs Unrated Performance:** 6.0% vs 20.3% failure rates
- **High-Risk Listings:** 10 listings with 32-35% failure rates need immediate attention
- **Market Concentration:** Top 5 orgs control 67.3% of business

### What Doesn't Matter
- Weather conditions (zero correlation)
- Day of week (2.6% variance)
- Seasonality (stable across 24 months)
- Customer ratings (don't drive volume, only quality)

### Data Coverage
- **Orders Data:** 100% coverage, fully reliable
- **Weather Data:** 93% coverage (good)
- **Rank/Uptime Data:** 5.7% coverage (insufficient for broad analysis)

---

## 📚 Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [SQL Style Guide](https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Business Insights Report](../../business_insights_report.md) - Detailed analysis of 2-year performance

---

**Last Updated:** {{ run_started_at }}

{% enddocs %}
