# Business Performance Analytics Pipeline

> A complete data engineering solution for analyzing restaurant delivery platform performance with weather correlation.

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Setup Instructions](#setup-instructions)
- [Security & Credentials](#security--credentials)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Usage](#usage)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project builds a **data pipeline** that:
1. ✅ Loads raw CSV data into PostgreSQL
2. ✅ Fetches weather data from Open-Meteo API via Airflow
3. ✅ Transforms data using DBT (cleaning, deduplication, aggregation)
4. ✅ Creates a final reporting table for business analytics
5. ✅ Implements data quality checks
6. ✅ Generates summary reports highlighting data issues

### Business Questions Answered
- How do daily orders correlate with weather conditions?
- Does platform ranking affect sales performance?
- How do ratings impact order volume?
- Which outlets/organizations perform best?

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐
│   CSV Files     │     │  Open-Meteo API  │
│  (Raw Data)     │     │ (Weather Data)   │
└────────┬────────┘     └────────┬─────────┘
         │                       │
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│         PostgreSQL Database             │
│  ┌────────┬─────────┬──────┬────────┐  │
│  │  raw   │ staging │ int  │  mart  │  │
│  └────────┴─────────┴──────┴────────┘  │
└─────────────────┬───────────────────────┘
                  │
         ┌────────┴────────┐
         │                 │
         ▼                 ▼
┌─────────────┐   ┌─────────────────┐
│  Airflow    │   │   DBT Models    │
│  (Weather)  │   │ (Transform)     │
└─────────────┘   └─────────┬───────┘
                            │
                            ▼
                  ┌─────────────────┐
                  │ fct_daily_      │
                  │ business_       │
                  │ performance     │
                  └─────────────────┘
```

**Data Flow:**
1. **Ingestion**: Python scripts load CSVs → PostgreSQL `raw` schema
2. **Orchestration**: Airflow DAG fetches weather data daily → `raw.weather` table
3. **Transformation**: DBT models transform data through layers:
   - `staging`: Deduplicated, cleaned data
   - `intermediate`: Business logic, calculations
   - `mart`: Final reporting table
4. **Quality**: DBT tests ensure data integrity
5. **Reporting**: Summary business_insights_report highlights issues

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | PostgreSQL 15 | Data storage |
| **Orchestration** | Apache Airflow 2.8 | Weather data pipeline |
| **Transformation** | DBT 1.7 | Data modeling |
| **Containerization** | Docker Compose | Local/cloud deployment |
| **Language** | Python 3.11 | Scripting, APIs |
| **API** | Open-Meteo | Weather data |

---


See [Detailed Setup Instructions](#detailed-setup-instructions) below.

---

## 🚀 Detailed Setup Instructions

### Prerequisites
- Docker Desktop installed
- Python 3.11, 3.12, or 3.13 installed
- 8GB RAM minimum
- 10GB free disk space


### 1. Clone/Download Project
```bash
cd c:\Project\data_engineering_work
```

### 2. Configure Environment Variables

#### **Option A: Quick Start (Local Development)**

For local testing with default credentials (not secure, but easy):

```bash
# Copy the environment template
cp .env.template .env

# Edit .env and uncomment the "QUICK START" section at the bottom
# These are insecure defaults - only use for local development!
```

#### **Option B: Secure Setup (Recommended)**

For production or if you want proper security:

```bash
# 1. Copy the environment template
cp .env.template .env

# 2. Generate secure secrets
python scripts/generate_secrets.py

# 3. Copy the generated values into your .env file
# Replace these placeholders:
#   - POSTGRES_PASSWORD=your_secure_password_here
#   - AIRFLOW__CORE__FERNET_KEY=GENERATE_NEW_KEY_HERE
#   - AIRFLOW__WEBSERVER__SECRET_KEY=GENERATE_NEW_KEY_HERE
#   - AIRFLOW_ADMIN_PASSWORD=your_airflow_password_here
#   - PGADMIN_DEFAULT_PASSWORD=your_pgadmin_password_here
```

#### **Configure DBT Profiles**

**Option A: Keep Default (Local Development)**

The project includes a pre-configured `dbt/profiles.yml` with default credentials. If you're using the quick start .env defaults, no changes needed.

**Option B: Use Custom Credentials (Recommended)**

If you changed the database password in `.env`:

```bash
# 1. Copy the DBT profiles template
cd dbt
cp profiles.yml.template profiles.yml

# 2. Edit profiles.yml and update:
#    - user: <your POSTGRES_USER>
#    - password: <your POSTGRES_PASSWORD>
```

**Option C: Use Environment Variables (Most Secure)**

Edit `dbt/profiles.yml`:
```yaml
business_performance:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: staging
      threads: 4
```

**Security Notes:**
- ⚠️ `.env` file is in `.gitignore` - it will NOT be committed to Git
- ⚠️ Never commit files with real passwords to version control
- ✅ Use `generate_secrets.py` to create cryptographically secure passwords


---

### 3. Setup Python Environment

**Run the automated setup script:**
```bash
# This will create venv, install dependencies, and verify everything
python setup.py
```

**The setup script will:**
- ✅ Check Python version (3.11+)
- ✅ Create virtual environment in `venv/`
- ✅ Install all dependencies from `requirements.txt`
- ✅ Install DBT with PostgreSQL adapter
- ✅ Verify Docker installation
- ✅ Create activation helper scripts

**Activate the virtual environment:**

**Windows:**
```bash
 venv\Scripts\activate
```

**Unix/Linux/Mac:**
```bash
 source venv/bin/activate
```

You should see `(venv)` in your terminal prompt when activated.

### 3. Start Infrastructure
```bash
# Start all containers
docker-compose up -d

# Wait 3-4 minutes for services to fully initialize
# (First startup takes longer - Docker downloads images)

# Check containers are running and healthy
docker-compose ps
```

**Expected output:**
```
NAME                   STATUS
business_postgres      Up (healthy)
business_pgadmin       Up
airflow_webserver      Up (healthy)
airflow_scheduler      Up
```

**Note:** Containers start in order: PostgreSQL → Airflow Webserver → Airflow Scheduler.

### 4. Access Services

**Note:** If you used custom credentials in Step 2, use those instead of the defaults below.

- **Airflow UI**: http://localhost:8080
  - Username: `admin` (or your `AIRFLOW_ADMIN_USER`)
  - Password: `admin` (or your `AIRFLOW_ADMIN_PASSWORD`)

- **pgAdmin** (Database UI): http://localhost:5050
  - Email: `admin@admin.com` (or your `PGADMIN_DEFAULT_EMAIL`)
  - Password: `admin` (or your `PGADMIN_DEFAULT_PASSWORD`)
  - See [pgAdmin Setup Guide](docs/PGADMIN_SETUP.md)

- **PostgreSQL**: `localhost:5432`
  - Username: `dataeng` (or your `POSTGRES_USER`)
  - Password: `dataeng123` (or your `POSTGRES_PASSWORD`)
  - Database: `business_db` (or your `POSTGRES_DB`)

### 5. Load CSV Data
```bash
# Make sure virtual environment is activated first!
python scripts/load_csv_data.py
```

### 6. Trigger Weather Data Collection
1. Go to Airflow UI (http://localhost:8080)
2. Find DAG: `fetch_weather_data`
3. Click **Trigger DAG** (play button) or  **Activate & Trigger** weather DAG (via Airflow UI or CLI)

```bash
docker exec airflow_scheduler airflow dags unpause fetch_weather_data

docker exec airflow_scheduler airflow dags trigger fetch_weather_data

```
4. Monitor progress in **Graph View**

### 7. Run DBT Models
```bash
# Virtual environment should already be activated
# DBT was installed during setup.py

# Navigate to DBT project
cd dbt

# Install utils 
dbt deps
# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve --port 8081   # Opens browser at http://localhost:8081
```

### 8. Verify Final Table
```sql
-- Connect to PostgreSQL
psql -h localhost -U dataeng -d business_db

-- Check reporting table
SELECT * FROM mart.fct_daily_business_performance LIMIT 10;
```

---

## 🔒 Security & Credentials

### Environment Variables

This project uses environment variables to manage credentials securely. **Never commit the `.env` file to Git** (it's already in `.gitignore`).

#### Files:
- **`.env`** - Your actual credentials (NOT in Git, created from template)
- **`.env.template`** - Template with placeholders (committed to Git)
- **`scripts/generate_secrets.py`** - Generates secure random passwords

#### Quick Reference:

| What | File | Committed to Git? |
|------|------|-------------------|
| Actual credentials | `.env` | ❌ NO (in .gitignore) |
| Template with placeholders | `.env.template` | ✅ YES |
| Secret generator script | `scripts/generate_secrets.py` | ✅ YES |

### DBT Profiles

DBT requires database credentials in `dbt/profiles.yml`:

#### Files:
- **`dbt/profiles.yml`** - Your actual DBT config (default: has dev credentials)
- **`dbt/profiles.yml.template`** - Template with placeholders

#### Security Options:

1. **Local Development (Default):**
   - Use the included `dbt/profiles.yml` with default credentials
   - Matches the Quick Start `.env` defaults
   - ⚠️ Only for local testing, not production

2. **Custom Credentials:**
   - Copy `profiles.yml.template` to `profiles.yml`
   - Update with your custom database credentials
   - If deploying to production, add `dbt/profiles.yml` to `.gitignore`

3. **Environment Variables (Most Secure):**
   - Use `{{ env_var('VARIABLE_NAME') }}` in `profiles.yml`
   - Credentials stored in `.env`, not in profiles.yml
   - Best for production deployments

### For Production Deployments

Before deploying to production or making your repository public:

1. **Generate secure secrets:**
   ```bash
   python scripts/generate_secrets.py
   ```

2. **Update all credentials** in your `.env` file

3. **Consider adding** `dbt/profiles.yml` **to .gitignore** if it contains real credentials

4. **Use proper secrets management:**
   - Docker Secrets
   - AWS Secrets Manager / Parameter Store
   - GCP Secret Manager
   - Azure Key Vault
   - Kubernetes Secrets


---

## 📁 Project Structure

```
data_engineering_work/
├── airflow/
│   ├── dags/
│   │   └── weather_data_dag.py          # Airflow DAG for weather API
│   ├── logs/                             # Airflow execution logs
│   └── plugins/                          # Custom Airflow plugins
│
├── data/                                 # Raw CSV files
│   ├── listing.csv                       # Restaurant listing data
│   ├── orders.csv                        # Individual order transactions
│   ├── orders_daily.csv                  # Pre-aggregated daily orders (snapshot)
│   ├── org.csv                           # Organization/company data
│   ├── outlet.csv                        # Physical outlet/location data
│   ├── platform.csv                      # Delivery platform data
│   ├── rank.csv                          # Platform ranking snapshots (hourly)
│   └── ratings_agg.csv                   # Customer ratings aggregates
│
├── dbt/                                  # DBT transformation project
│   ├── models/
│   │   ├── staging/                      # Staging layer (deduplication, cleaning)
│   │   │   ├── sources.yml               # Source table definitions
│   │   │   ├── schema.yml                # Tests and documentation
│   │   │   ├── stg_listing.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_orders_daily.sql
│   │   │   ├── stg_org.sql
│   │   │   ├── stg_outlet.sql
│   │   │   ├── stg_platform.sql
│   │   │   ├── stg_rank.sql
│   │   │   ├── stg_ratings_agg.sql
│   │   │   └── stg_weather.sql
│   │   ├── intermediate/                 # Business logic transformations
│   │   │   ├── schema.yml                # Tests and documentation
│   │   │   ├── int_orders_enriched.sql   # Orders + dimensions
│   │   │   ├── int_ratings_metrics.sql   # Rating trends + deltas
│   │   │   └── int_rank_daily_avg.sql    # Daily rank averages + uptime
│   │   └── mart/                         # Final reporting tables
│   │       ├── schema.yml                # Tests and documentation
│   │       └── fct_daily_business_performance.sql  # Main fact table
│   ├── dbt_project.yml                   # DBT project configuration
│   ├── profiles.yml                      # Database connection (default creds)
│   ├── profiles.yml.template             # Template for custom credentials
│   └── packages.yml                      # DBT package dependencies
│
├── docs/                                 # Documentation
│   ├── ARCHITECTURE_DIAGRAM.md           # System architecture overview
│   ├── PGADMIN_SETUP.md                  # pgAdmin connection guide
│ 
│
├── scripts/                              # Python utility scripts
│   ├── db_utils.py                       # Database connection utilities
│   ├── generate_secrets.py               # Secure password generator
│   ├── load_csv_data.py                  # Load CSVs to PostgreSQL
│   └── weather_api.py                    # Weather API client
│
├── sql/
│   └── init.sql                          # PostgreSQL schema initialization
│
├── tests/
│   └── test_data_quality.py              # Python data quality tests
│
├── .env                                   # Environment variables (NOT in Git)
├── .env.template                          # Environment template (in Git)
├── .gitignore                            # Git ignore rules
├── business_insights_report.md            # Business analytics report
├── docker-compose.yml                     # Infrastructure definition
├── Dockerfile.airflow                     # Custom Airflow image
├── README.md                             # This file
├── requirements.txt                       # Python dependencies
├── requirements-airflow.txt               # Airflow-specific dependencies
└── setup.py                              # Automated environment setup script
```

---

## 🗄️ Data Model

### Medallion Architecture

This project follows the **Medallion Architecture** pattern (Bronze → Silver → Gold) for data quality and organization:

```
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw)                      │
│  Raw CSV loads + Airflow weather data - No transformations  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Staging)                    │
│     Cleaned, deduplicated, type-cast, validated data        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                SILVER LAYER (Intermediate)                  │
│        Business logic, calculations, enrichments            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Mart)                        │
│       Final fact table for analytics and reporting          │
└─────────────────────────────────────────────────────────────┘
```

---

### Schema Layers

#### 1. **Raw Schema** (Bronze Layer)

Direct CSV loads with no transformations - preserved exactly as received:

| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `raw.listing` | listing.csv | ~200 | Restaurant listings on platforms |
| `raw.orders` | orders.csv | ~96K | Individual order transactions |
| `raw.orders_daily` | orders_daily.csv | ~43K | Pre-aggregated daily snapshot (deprecated) |
| `raw.org` | org.csv | 10 | Parent organizations |
| `raw.outlet` | outlet.csv | ~50 | Physical outlet locations |
| `raw.platform` | platform.csv | 5 | Delivery platforms (JustEat, DoorDash, etc.) |
| `raw.rank` | rank.csv | ~15K | Hourly platform ranking snapshots |
| `raw.ratings_agg` | ratings_agg.csv | ~900 | Customer rating aggregates |
| `raw.weather` | Airflow API | ~58K | Hourly weather data from Open-Meteo |

**Key Characteristics:**
- No data cleaning or validation
- Preserves original data types and nulls
- Contains duplicates and data quality issues
- Used only as source for staging layer

---

#### 2. **Staging Schema** (Silver Layer)

Cleaned and deduplicated data ready for business logic:

| Model | Purpose | Transformations |
|-------|---------|-----------------|
| `stg_listing` | Cleaned listings | Dedupe by listing_id (latest timestamp) |
| `stg_orders` | Cleaned orders | Validate order_id uniqueness, filter valid statuses |
| `stg_orders_daily` | Cleaned snapshots | Dedupe, filter negative values (deprecated source) |
| `stg_org` | Cleaned organizations | Dedupe by org_id |
| `stg_outlet` | Cleaned outlets | Dedupe by outlet_id, validate coordinates |
| `stg_platform` | Cleaned platforms | Dedupe by platform_id |
| `stg_rank` | Cleaned rankings | Keep all records (online + offline) for uptime tracking |
| `stg_ratings_agg` | Cleaned ratings | Dedupe by date + listing_id |
| `stg_weather` | Cleaned weather | Dedupe by datetime + outlet_id |

**Deduplication Strategy:**
```sql
row_number() over (
    partition by <unique_key>
    order by timestamp desc  -- Keep latest record
) = 1
```

**Data Quality:**
- Removed duplicates
- Filtered invalid/negative values
- Type casting (dates, timestamps, numerics)
- NULL handling for optional fields

---

#### 3. **Intermediate Schema** (Silver Layer)

Business logic transformations and enrichments:

**`int_orders_enriched`**
- Orders joined with all dimensions (listing, outlet, org, platform)
- Grain: One row per order
- Use: Foundation for order aggregations

**`int_ratings_metrics`**
- Cumulative rating count per listing over time
- Rating delta from previous day
- Rating trend classification (improving/declining/stable/new)
- Grain: One row per date + listing_id

**`int_rank_daily_avg`**
- Daily average rank from hourly snapshots
- Uptime percentage (% of time online)
- Availability category (Always Online, Mostly Online, etc.)
- Rank volatility metrics
- Grain: One row per date + listing_id

---

#### 4. **Mart Schema** (Gold Layer)

Final denormalized fact table for analytics:

### `mart.fct_daily_business_performance`

**Grain:** One row per date + listing_id (daily grain)

**Column Groups:**

| Group | Columns | Description |
|-------|---------|-------------|
| **Temporal** | `date` | Daily grain |
| **Keys** | `listing_id`, `outlet_id`, `org_id`, `platform_id` | Foreign keys |
| **Listing Attributes** | `listing_timestamp` | Listing metadata timestamp |
| **Outlet Attributes** | `outlet_name`, `latitude`, `longitude`, `has_valid_coordinates` | Physical location info |
| **Organization** | `org_name` | Parent company |
| **Platform** | `platform_name`, `platform_group`, `platform_country` | Delivery platform details |
| **Order Metrics** | `daily_orders`, `daily_orders_from_snapshot`, `completed_orders`, `pending_orders`, `cancelled_orders`, `refunded_orders` | Order volumes by status |
| **Order Derived** | `successful_orders`, `failed_orders`, `order_failure_rate` | Calculated metrics |
| **Data Quality** | `data_quality_flag`, `snapshot_vs_actual_diff` | Source comparison indicators |
| **Rating Metrics** | `daily_new_ratings`, `cumulative_rating_count`, `avg_rating`, `rating_delta`, `rating_trend`, `rating_category` | Customer satisfaction |
| **Rank Metrics** | `avg_rank`, `daily_best_rank`, `daily_worst_rank`, `rank_volatility` | Platform visibility |
| **Rank Readings** | `rank_total_readings`, `rank_online_readings`, `rank_offline_readings` | Tracking counts |
| **Uptime Metrics** | `uptime_percentage`, `was_ever_online`, `availability_category` | Online status |
| **Weather Metrics** | `avg_temperature`, `avg_humidity`, `avg_wind_speed`, `min_temperature`, `max_temperature` | Weather conditions |
| **Weather Status** | `weather_data_complete`, `temperature_category` | Data completeness |
| **Anomaly Flags** | `is_offline_order_anomaly`, `is_high_failure_when_offline` | Data quality indicators |

**Data Quality Features:**
- **Dual Source Comparison:** Maintains both `daily_orders` (transactional - source of truth) and `daily_orders_from_snapshot` (deprecated) for transparency
- **Quality Flags:** Automatic categorization of discrepancies (match, minor_discrepancy, large_discrepancy)
- **Anomaly Detection:** Identifies orders while offline, high failures during offline periods
- **99.98% Match Rate:** Between transactional and snapshot sources where both exist

**Total Columns:** 51

**Total Rows:** ~62,569 (200 listings × 731 days, with sparse data for some dimensions)

**Use Cases:**
- Business performance analysis
- Weather correlation studies
- Rating impact analysis
- Platform ranking effectiveness
- Data quality monitoring
- Operational anomaly detection

---

### Data Lineage

```
CSV Files ──┐
            ├──> Raw Schema ──> Staging Schema ──> Intermediate ──> Mart (Fact Table)
Airflow API─┘
```

**Query the lineage:**
```bash
cd dbt
dbt docs generate
dbt docs serve  # View interactive lineage graph
```

---

## 💻 Usage

### Running the Full Pipeline

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Load CSV data
python scripts/load_csv_data.py

# 3. Activate & Trigger weather DAG (via Airflow UI or CLI)
docker exec airflow_scheduler airflow dags unpause fetch_weather_data

docker exec airflow_scheduler airflow dags trigger fetch_weather_data

# 4. Run DBT models
cd dbt && dbt run

# 5. Run tests
dbt test


```

### Common DBT Commands

```bash
# Run specific model
dbt run --select stg_listing

# Run models and downstream dependencies
dbt run --select stg_listing+

# Test specific model
dbt test --select stg_listing

# Dry run (compile SQL only)
dbt compile

# Fresh build (drop and recreate)
dbt run --full-refresh
```

---

## 🧪 Testing

### Data Quality Checks

DBT tests are defined in:
- `models/staging/*.yml` (schema tests)
- `tests/` (custom tests)

**Test Types:**
1. **Uniqueness**: Primary keys are unique
2. **Not Null**: Required fields are populated
3. **Relationships**: Foreign keys reference valid records
4. **Accepted Values**: Status fields contain valid values
5. **Custom Tests**: Date ranges, duplicates, outliers

### Running Tests

```bash
# All tests
dbt test

# Specific model tests
dbt test --select stg_orders

# Store failures for analysis
dbt test --store-failures
```

---

## 🔍 Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs postgres
docker-compose logs airflow-webserver

# Restart containers
docker-compose restart
```

### Database connection failed
```bash
# Test connection
psql -h localhost -U dataeng -d business_db

# Check PostgreSQL is healthy
docker-compose ps
```

### Airflow DAG not appearing
```bash
# Check DAG syntax
docker exec airflow_scheduler airflow dags list

# View DAG errors
docker exec airflow_scheduler airflow dags list-import-errors
```

### DBT connection error
```bash
# Test connection
dbt debug

# Check profiles.yml settings
cat dbt/profiles.yml
```

---

## 📊 Sample Queries

```sql
-- Daily performance summary
SELECT
    date,
    COUNT(DISTINCT listing_id) as active_listings,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as avg_rating,
    AVG(avg_temperature) as avg_temp
FROM mart.fct_daily_business_performance
GROUP BY date
ORDER BY date DESC;

-- Weather correlation with orders
SELECT
    CASE
        WHEN avg_temperature < 10 THEN 'Cold'
        WHEN avg_temperature < 20 THEN 'Mild'
        ELSE 'Warm'
    END as temp_range,
    AVG(daily_orders) as avg_orders
FROM mart.fct_daily_business_performance
GROUP BY 1;
```

---


**Technologies used:**
- Docker & Docker Compose
- PostgreSQL
- Apache Airflow
- DBT
- Python (pandas, requests, sqlalchemy)
- SQL (window functions, CTEs, joins)

---


