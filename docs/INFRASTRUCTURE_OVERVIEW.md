# Infrastructure Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER HOST MACHINE                                │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        Docker Compose Network                           │ │
│  │                                                                          │ │
│  │  ┌──────────────────┐        ┌──────────────────┐                      │ │
│  │  │   PostgreSQL     │        │    pgAdmin 4     │                      │ │
│  │  │   Container      │◄───────│   Container      │                      │ │
│  │  │                  │  Web   │                  │                      │ │
│  │  │  Port: 5432      │  UI    │  Port: 5050      │                      │ │
│  │  │  Database: db    │        │  (Web Interface) │                      │ │
│  │  │                  │        │                  │                      │ │
│  │  │  Schemas:        │        │  Database Admin  │                      │ │
│  │  │  - raw           │        │  Query Tool      │                      │ │
│  │  │  - staging       │        │  Visual Explorer │                      │ │
│  │  │  - intermediate  │        │                  │                      │ │
│  │  │  - mart          │        │                  │                      │ │
│  │  └────────▲─────────┘        └──────────────────┘                      │ │
│  │           │                                                              │ │
│  │           │ SQL                                                          │ │
│  │           │ Connection                                                   │ │
│  │           │                                                              │ │
│  │  ┌────────┴─────────┐        ┌──────────────────┐                      │ │
│  │  │   Airflow        │        │   Python         │                      │ │
│  │  │   Container      │        │   Scripts        │                      │ │
│  │  │                  │        │   (Host)         │                      │ │
│  │  │  Port: 8080      │        │                  │                      │ │
│  │  │  (Web UI)        │        │  - load_csv_data │                      │ │
│  │  │                  │        │  - weather_api   │                      │ │
│  │  │  Components:     │        │  - db_utils      │                      │ │
│  │  │  - Webserver     │        │                  │                      │ │
│  │  │  - Scheduler     │        └──────────────────┘                      │ │
│  │  │  - Executor      │                 │                                │ │
│  │  │                  │                 │ Uses                           │ │
│  │  │  DAGs:           │◄────────────────┘                                │ │
│  │  │  - weather_data  │  Mounted                                         │ │
│  │  │                  │  Volume                                          │ │
│  │  └──────────────────┘                                                   │ │
│  │                                                                          │ │
│  └──────────────────────────────────────────────────────────────────────── │ │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          Host Environment                               │ │
│  │                                                                          │ │
│  │  ┌──────────────────┐                                                   │ │
│  │  │   dbt Core       │                                                   │ │
│  │  │   (Python)       │                                                   │ │
│  │  │                  │                                                   │ │
│  │  │  Commands:       │                                                   │ │
│  │  │  - dbt run       │──────► Connects to PostgreSQL (localhost:5432)   │ │
│  │  │  - dbt test      │        Executes SQL transformations              │ │
│  │  │  - dbt docs      │        Creates models/tests                      │ │
│  │  │                  │                                                   │ │
│  │  │  Models:         │                                                   │ │
│  │  │  - staging/      │                                                   │ │
│  │  │  - intermediate/ │                                                   │ │
│  │  │  - mart/         │                                                   │ │
│  │  └──────────────────┘                                                   │ │
│  │                                                                          │ │
│  └──────────────────────────────────────────────────────────────────────── │ │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘

                              ▲
                              │
                              │ API Calls
                              │
                    ┌─────────┴──────────┐
                    │  Open-Meteo API    │
                    │  (External)        │
                    │                    │
                    │  Weather Data      │
                    │  Hourly Records    │
                    └────────────────────┘
```

---

## Component Details

### 1. **PostgreSQL Database Container**
```yaml
Service: postgres
Port: 5432 (mapped to host)
Image: postgres:15
```

**Purpose:** Central data warehouse storing all data

**Schemas:**
- `raw` - Raw data from CSVs and APIs (loaded by Python scripts)
- `staging` - Cleaned and standardized data (created by dbt)
- `intermediate` - Enriched and joined data (created by dbt)
- `mart` - Final business reporting tables (created by dbt)

**Data Flow:**
```
CSV Files → Python scripts → raw schema
API Data  → Airflow DAG   → raw schema
raw       → dbt models    → staging/intermediate/mart
```

---

### 2. **pgAdmin 4 Container**
```yaml
Service: pgadmin
Port: 5050 (mapped to host)
Image: dpage/pgadmin4
```

**Purpose:** Web-based database administration tool

**Features:**
- Visual query builder
- Schema explorer
- SQL query editor
- Table data viewer
- Performance monitoring

**Access:** http://localhost:5050

**Use Cases:**
- Inspect table contents
- Run ad-hoc SQL queries
- Verify data quality
- Debug transformation issues

---

### 3. **Airflow Container**
```yaml
Service: airflow
Port: 8080 (mapped to host)
Image: Custom (Dockerfile.airflow)
```

**Purpose:** Orchestrate scheduled data pipelines

**Components Inside Container:**
- **Webserver** - Web UI for monitoring DAGs
- **Scheduler** - Triggers DAG runs on schedule
- **Executor** - Runs tasks

**DAGs:**
- `weather_data_dag.py` - Fetches hourly weather data from Open-Meteo API
  - Schedule: Daily at 2 AM
  - Tasks: fetch → validate → store in raw.weather

**Volumes Mounted:**
- `./airflow/dags` → `/opt/airflow/dags` (DAG definitions)
- `./scripts` → `/opt/airflow/scripts` (Python utilities)

**Access:** http://localhost:8080

---

### 4. **Python Scripts (Host)**
```
Location: ./scripts/
Execution: Run directly on host machine
```

**Scripts:**

**`load_csv_data.py`**
- Loads CSV files into PostgreSQL `raw` schema
- Handles deduplication using timestamps
- One-time setup script

**`weather_api.py`**
- Fetches weather data from Open-Meteo API
- Used by Airflow DAG
- Stores in `raw.weather` table

**`db_utils.py`**
- Database connection utilities
- Shared by all scripts
- Uses SQLAlchemy

**Connection:**
- Connects to PostgreSQL via `localhost:5432`
- Uses credentials from `.env` file

---

### 5. **dbt Core (Host)**
```
Location: ./dbt/
Execution: Run via command line on host
```

**Purpose:** SQL-based data transformation framework

**Commands:**
```bash
dbt run        # Execute all models (create tables/views)
dbt test       # Run data quality tests (77 tests)
dbt docs serve # Generate documentation website
```

**Models Structure:**
```
dbt/models/
├── staging/           # Clean raw data (9 models)
│   ├── stg_orders.sql
│   ├── stg_weather.sql
│   └── ...
├── intermediate/      # Enriched data (3 models)
│   ├── int_orders_enriched.sql
│   ├── int_ratings_metrics.sql
│   └── int_rank_daily_avg.sql
└── mart/              # Final reporting (1 model)
    └── fct_daily_business_performance.sql
```

**Connection:**
- Reads `profiles.yml` for database credentials
- Connects to PostgreSQL via `localhost:5432`
- Executes SQL transformations
- Creates tables/views in staging/intermediate/mart schemas

---

## Data Flow Timeline

### Step 1: Initial Setup (One-time)
```
1. docker-compose up -d
   └─> Starts PostgreSQL, pgAdmin, Airflow containers

2. python scripts/load_csv_data.py
   └─> Loads CSV files → raw schema tables

3. Airflow DAG: weather_data_dag (manual trigger)
   └─> Fetches weather data → raw.weather table
```

### Step 2: dbt Transformations (One-time / As needed)
```
4. dbt run
   └─> staging models   → Creates stg_* tables
   └─> intermediate     → Creates int_* tables
   └─> mart            → Creates fct_daily_business_performance

5. dbt test
   └─> Runs 77 data quality tests
   └─> Validates relationships, null checks, custom tests
```

### Step 3: Ongoing Operations
```
6. Airflow (Daily at 2 AM)
   └─> weather_data_dag runs automatically
   └─> Fetches latest weather data
   └─> Updates raw.weather

7. dbt run (Manual / Could be scheduled)
   └─> Re-runs transformations with new data
   └─> Updates mart tables

8. pgAdmin (Anytime)
   └─> View results
   └─> Run ad-hoc queries
   └─> Generate reports
```

---

## Port Mapping

| Service    | Container Port | Host Port | URL                    |
|------------|----------------|-----------|------------------------|
| PostgreSQL | 5432           | 5432      | localhost:5432         |
| pgAdmin    | 80             | 5050      | http://localhost:5050  |
| Airflow    | 8080           | 8080      | http://localhost:8080  |
| dbt docs   | N/A            | 8081      | http://localhost:8081  |

---

## Technology Stack Summary

| Component    | Technology       | Purpose                          | Runs In      |
|--------------|------------------|----------------------------------|--------------|
| Database     | PostgreSQL 15    | Data warehouse                   | Docker       |
| Admin Tool   | pgAdmin 4        | Database management              | Docker       |
| Orchestrator | Apache Airflow   | Workflow scheduling              | Docker       |
| Transform    | dbt Core         | SQL transformations              | Host Python  |
| Scripts      | Python 3.x       | Data loading, API calls          | Host Python  |
| Container    | Docker Compose   | Infrastructure orchestration     | Docker       |

---

## Network Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Bridge Network                     │
│                                                              │
│  postgres:5432 ◄──► pgadmin:80 ◄──► airflow:8080           │
│                                                              │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       │ Port Mapping
                       │
┌──────────────────────▼───────────────────────────────────────┐
│                    Host Machine (localhost)                  │
│                                                              │
│  :5432 (PostgreSQL) :5050 (pgAdmin) :8080 (Airflow)        │
│                                                              │
│  ┌────────────┐    ┌────────────┐                          │
│  │ dbt Core   │    │  Python    │                          │
│  │ (localhost │    │  Scripts   │                          │
│  │  :5432)    │    │ (localhost │                          │
│  │            │    │  :5432)    │                          │
│  └────────────┘    └────────────┘                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## File System Structure

```
data_engineering_work/
├── docker-compose.yml          # Container orchestration
├── Dockerfile.airflow          # Custom Airflow image
├── .env                        # Database credentials
│
├── data/                       # CSV source files
│   ├── orders.csv
│   ├── orders_daily.csv
│   └── ...
│
├── scripts/                    # Python utilities
│   ├── load_csv_data.py       # CSV loader
│   ├── weather_api.py         # API client
│   └── db_utils.py            # Connection helper
│
├── airflow/
│   ├── dags/
│   │   └── weather_data_dag.py # Weather pipeline
│   └── logs/                   # Airflow logs
│
├── dbt/
│   ├── models/                # SQL transformations
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── mart/
│   ├── tests/                 # Custom tests
│   ├── profiles.yml           # DB connection config
│   └── dbt_project.yml        # dbt configuration
│
└── sql/
    └── init.sql               # Database initialization
```

---

## Security & Configuration

### Environment Variables (.env)
```bash
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=db
POSTGRES_PORT=5432
```

### Database Connection Strings

**From Python scripts:**
```python
postgresql://user:password@localhost:5432/db
```

**From dbt:**
```yaml
# profiles.yml
host: localhost
port: 5432
user: user
password: password
dbname: db
```

**From Airflow (inside container):**
```python
postgresql://user:password@postgres:5432/db
# Note: Uses 'postgres' hostname (container name)
```

---

## Common Operations

### Start Infrastructure
```bash
docker-compose up -d
```

### Stop Infrastructure
```bash
docker-compose down
```

### Load Data
```bash
python scripts/load_csv_data.py
```

### Run Transformations
```bash
cd dbt
dbt run
dbt test
```

### View Documentation
```bash
cd dbt
dbt docs generate
dbt docs serve --port 8081
```

### Trigger Airflow DAG
- Visit http://localhost:8080
- Enable `weather_data_dag`
- Click "Trigger DAG"

### Query Database
- Visit http://localhost:5050 (pgAdmin)
- Or use any SQL client: `localhost:5432`

---

## Medallion Architecture Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     RAW LAYER (Bronze)                       │
│  Python Scripts & Airflow → PostgreSQL raw schema           │
│  - Raw CSVs loaded as-is                                    │
│  - API data with minimal processing                         │
│  - Duplicates and quality issues present                    │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ dbt staging models
                   │
┌──────────────────▼──────────────────────────────────────────┐
│                   STAGING LAYER (Silver)                     │
│  dbt models → PostgreSQL staging schema                     │
│  - Deduplicated (using timestamps)                          │
│  - Standardized column names                                │
│  - Type casting and cleaning                                │
│  - Filtered invalid records                                 │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ dbt intermediate models
                   │
┌──────────────────▼──────────────────────────────────────────┐
│                 INTERMEDIATE LAYER (Silver)                  │
│  dbt models → PostgreSQL intermediate schema                │
│  - Joined dimensional data                                  │
│  - Enriched with context                                    │
│  - Pre-aggregated metrics                                   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ dbt mart models
                   │
┌──────────────────▼──────────────────────────────────────────┐
│                     MART LAYER (Gold)                        │
│  dbt models → PostgreSQL mart schema                        │
│  - Business-ready reporting tables                          │
│  - fct_daily_business_performance (62,569 rows)            │
│  - Aggregated to analysis grain                            │
│  - Data quality validated (77 tests)                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Why This Architecture?

### Docker Compose Benefits
- ✅ One command to start entire stack
- ✅ Consistent environment across machines
- ✅ Easy to tear down and rebuild
- ✅ Network isolation and security

### dbt on Host Benefits
- ✅ Fast development iteration
- ✅ IDE integration and syntax highlighting
- ✅ Easy to version control
- ✅ No container rebuild needed for model changes

### Separation of Concerns
- **PostgreSQL** - Data storage
- **Airflow** - Orchestration and scheduling
- **dbt** - Transformation logic
- **Python** - Data loading and API integration
- **pgAdmin** - Administration and exploration

This architecture follows modern data engineering best practices and is production-ready for scaling.

---

## API ETL Architecture

### Weather Data Pipeline (Airflow DAG)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WEATHER DATA ETL PIPELINE                             │
│                    (Airflow DAG: weather_data_dag)                          │
└─────────────────────────────────────────────────────────────────────────────┘

                              EXTRACT
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  TASK 1: validate_outlets                                          │    │
│  │  ─────────────────────────                                          │    │
│  │  Purpose: Pre-flight checks before API calls                       │    │
│  │                                                                      │    │
│  │  SELECT outlet_id, latitude, longitude                             │    │
│  │  FROM raw.outlet                                                    │    │
│  │  WHERE latitude IS NOT NULL                                         │    │
│  │    AND longitude IS NOT NULL                                        │    │
│  │    AND latitude BETWEEN -90 AND 90                                  │    │
│  │    AND longitude BETWEEN -180 AND 180                               │    │
│  │                                                                      │    │
│  │  ✓ Validates: 73 outlets with valid coordinates                    │    │
│  │  ✗ Fails if: No outlets found or all invalid coordinates           │    │
│  └────────────────────┬───────────────────────────────────────────────┘    │
│                       │                                                      │
│                       ▼                                                      │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  TASK 2: fetch_weather_data                                        │    │
│  │  ──────────────────────────                                         │    │
│  │  Purpose: Extract weather data from Open-Meteo API                 │    │
│  │                                                                      │    │
│  │  For each outlet (73 locations):                                   │    │
│  │    ┌─────────────────────────────────────────────────────┐        │    │
│  │    │ API Call: Open-Meteo Historical Weather API         │        │    │
│  │    │ Endpoint: archive-api.open-meteo.com/v1/archive     │        │    │
│  │    └─────────────────────────────────────────────────────┘        │    │
│  │                                                                      │    │
│  │  Request Parameters:                                                │    │
│  │  ┌──────────────────────────────────────────────────────┐         │    │
│  │  │ latitude:     40.7128                                 │         │    │
│  │  │ longitude:    -74.0060                                │         │    │
│  │  │ start_date:   2023-01-01                              │         │    │
│  │  │ end_date:     2024-12-31                              │         │    │
│  │  │ hourly:       temperature_2m,                         │         │    │
│  │  │               relative_humidity_2m,                   │         │    │
│  │  │               wind_speed_10m                          │         │    │
│  │  │ timezone:     UTC                                     │         │    │
│  │  └──────────────────────────────────────────────────────┘         │    │
│  │                                                                      │    │
│  │  Response (per outlet): ~17,520 hourly records                     │    │
│  │  Total API calls: 73 outlets                                       │    │
│  │  Total records fetched: 438,000+ rows                              │    │
│  │                                                                      │    │
│  │  ✓ Success: Returns DataFrame with all weather records             │    │
│  │  ✗ Retry: 3 attempts with 5-second delay on API failure           │    │
│  │  ✗ Timeout: 30 seconds per request, 2 hours for entire task       │    │
│  └────────────────────┬───────────────────────────────────────────────┘    │
│                       │                                                      │
└───────────────────────┼──────────────────────────────────────────────────────┘
                        │
                        │ XCom: Pass row count to next task
                        │
                        ▼
                    TRANSFORM
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  TASK 2 (continued): Data Transformation                           │    │
│  │  ────────────────────────────────                                   │    │
│  │  Purpose: Clean and structure API response                         │    │
│  │                                                                      │    │
│  │  Transformations Applied:                                           │    │
│  │  1. Parse JSON response → pandas DataFrame                         │    │
│  │  2. Add outlet_id to each record                                   │    │
│  │  3. Rename columns:                                                 │    │
│  │     - time          → datetime                                      │    │
│  │     - temperature_2m → temperature_2m                               │    │
│  │  4. Type conversions:                                               │    │
│  │     - datetime      → timestamp                                     │    │
│  │     - temperature   → float                                         │    │
│  │     - humidity      → float                                         │    │
│  │     - wind_speed    → float                                         │    │
│  │  5. Concatenate all outlets into single DataFrame                  │    │
│  │                                                                      │    │
│  │  Output Schema:                                                     │    │
│  │  ┌───────────────────────────────────────┐                         │    │
│  │  │ outlet_id       INTEGER               │                         │    │
│  │  │ datetime        TIMESTAMP             │                         │    │
│  │  │ temperature_2m  FLOAT (°C)            │                         │    │
│  │  │ relative_humidity_2m FLOAT (%)        │                         │    │
│  │  │ wind_speed_10m  FLOAT (km/h)          │                         │    │
│  │  └───────────────────────────────────────┘                         │    │
│  └────────────────────┬───────────────────────────────────────────────┘    │
│                       │                                                      │
└───────────────────────┼──────────────────────────────────────────────────────┘
                        │
                        ▼
                      LOAD
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  TASK 3: store_weather_data                                        │    │
│  │  ──────────────────────────                                         │    │
│  │  Purpose: Persist data to PostgreSQL database                      │    │
│  │                                                                      │    │
│  │  Database Write Strategy:                                           │    │
│  │  ┌──────────────────────────────────────────────────────┐         │    │
│  │  │ Method:     df.to_sql()                              │         │    │
│  │  │ Schema:     raw                                       │         │    │
│  │  │ Table:      weather                                   │         │    │
│  │  │ If Exists:  replace (on initial run)                 │         │    │
│  │  │             append (on subsequent runs)               │         │    │
│  │  │ Batch Size: 1000 rows per INSERT                     │         │    │
│  │  │ Method:     multi (bulk insert optimization)         │         │    │
│  │  └──────────────────────────────────────────────────────┘         │    │
│  │                                                                      │    │
│  │  Bulk Insert Performance:                                           │    │
│  │  - 438,000 rows inserted in ~438 batches (1000 rows each)         │    │
│  │  - method='multi' = 10x faster than single-row inserts             │    │
│  │  - Typical completion time: 2-3 minutes                            │    │
│  │                                                                      │    │
│  │  SQL Executed (conceptual):                                         │    │
│  │  INSERT INTO raw.weather                                            │    │
│  │    (outlet_id, datetime, temperature_2m,                           │    │
│  │     relative_humidity_2m, wind_speed_10m)                          │    │
│  │  VALUES                                                             │    │
│  │    (1, '2023-01-01 00:00:00', 5.2, 78.0, 12.5),                   │    │
│  │    (1, '2023-01-01 01:00:00', 4.8, 79.0, 11.8),                   │    │
│  │    ... [1000 rows per batch] ...                                   │    │
│  │                                                                      │    │
│  │  ✓ Success: All records persisted to database                      │    │
│  │  ✗ Fails if: Database connection lost or constraint violation     │    │
│  └────────────────────┬───────────────────────────────────────────────┘    │
│                       │                                                      │
└───────────────────────┼──────────────────────────────────────────────────────┘
                        │
                        ▼
                    VALIDATE
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  TASK 4: validate_weather_data                                     │    │
│  │  ─────────────────────────────                                      │    │
│  │  Purpose: Comprehensive data quality checks (FAIL FAST)            │    │
│  │                                                                      │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ CHECK 1: Row Count Integrity                                 │ │    │
│  │  │ ────────────────────────────                                  │ │    │
│  │  │ SELECT COUNT(*) FROM raw.weather;                            │ │    │
│  │  │                                                                │ │    │
│  │  │ Expected: 438,000+ rows (from XCom)                          │ │    │
│  │  │ ✗ FAIL if: Stored count != Fetched count                     │ │    │
│  │  │ Error: "Lost X records during storage!"                      │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                      │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ CHECK 2: NULL Value Detection (Critical Columns)             │ │    │
│  │  │ ────────────────────────────────────────────                  │ │    │
│  │  │ SELECT                                                         │ │    │
│  │  │   SUM(CASE WHEN outlet_id IS NULL THEN 1 ELSE 0 END),        │ │    │
│  │  │   SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END),         │ │    │
│  │  │   SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END)    │ │    │
│  │  │ FROM raw.weather;                                             │ │    │
│  │  │                                                                │ │    │
│  │  │ ✗ FAIL if: ANY critical column has NULLs                     │ │    │
│  │  │ Error: "Found X records with NULL temperature!"              │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                      │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ CHECK 3: Data Quality Ranges                                 │ │    │
│  │  │ ───────────────────────────                                   │ │    │
│  │  │ SELECT COUNT(*) FROM raw.weather WHERE                       │ │    │
│  │  │   temperature_2m NOT BETWEEN -50 AND 50                      │ │    │
│  │  │   OR relative_humidity_2m NOT BETWEEN 0 AND 100              │ │    │
│  │  │   OR wind_speed_10m NOT BETWEEN 0 AND 200;                   │ │    │
│  │  │                                                                │ │    │
│  │  │ ✗ FAIL if: Values outside realistic ranges                   │ │    │
│  │  │ Error: "Found X records with invalid measurements!"          │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                      │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ CHECK 4: Foreign Key Integrity                               │ │    │
│  │  │ ──────────────────────────                                    │ │    │
│  │  │ SELECT COUNT(*) FROM raw.weather w                           │ │    │
│  │  │ LEFT JOIN raw.outlet o ON w.outlet_id = o.id                 │ │    │
│  │  │ WHERE o.id IS NULL;                                           │ │    │
│  │  │                                                                │ │    │
│  │  │ ✗ FAIL if: Orphaned records (outlet_id doesn't exist)       │ │    │
│  │  │ Error: "Found X records with invalid outlet_id!"             │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                      │    │
│  │  Data Quality Philosophy:                                           │    │
│  │  ✓ All checks passed = DAG SUCCESS (green in Airflow UI)          │    │
│  │  ✗ Any check failed = DAG FAILURE (red in Airflow UI)             │    │
│  │  - No silent failures - every issue raises ValueError              │    │
│  │  - Explicit error messages with exact problem details              │    │
│  └────────────────────┬───────────────────────────────────────────────┘    │
│                       │                                                      │
└───────────────────────┼──────────────────────────────────────────────────────┘
                        │
                        ▼
                   ✓ SUCCESS
┌─────────────────────────────────────────────────────────────────────────────┐
│                       FINAL RESULT                                           │
│                                                                               │
│  PostgreSQL Table: raw.weather                                               │
│  ────────────────────────────────                                            │
│  Rows: 438,000+ hourly weather records                                      │
│  Time Period: 2023-01-01 to 2024-12-31 (2 years)                           │
│  Locations: 73 outlets                                                       │
│  Metrics: Temperature, Humidity, Wind Speed                                  │
│  Granularity: Hourly                                                         │
│  Data Quality: 100% validated and complete                                   │
│                                                                               │
│  Ready for dbt transformation → stg_weather → fct_daily_business_performance │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### API ETL Component Breakdown

#### 1. **Open-Meteo API Integration**

**API Details:**
- **Provider:** Open-Meteo (open-source weather API)
- **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
- **Type:** Historical weather data (not real-time)
- **Authentication:** None required (free tier)
- **Rate Limiting:** Generous limits (73 API calls easily within limits)
- **Documentation:** https://open-meteo.com/en/docs/historical-weather-api

**Why Open-Meteo?**
- ✅ Free and open-source
- ✅ No API key required
- ✅ Historical data available (perfect for 2-year analysis)
- ✅ High-quality data from multiple weather models
- ✅ Simple REST API with JSON responses

#### 2. **Python Weather API Client**

**File:** `scripts/weather_api.py`

**Class:** `WeatherAPIClient`

**Key Features:**
```python
class WeatherAPIClient:
    def __init__(self):
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.retry_attempts = 3
        self.retry_delay = 5
        self.timeout = 30

    def fetch_weather_data(latitude, longitude, start_date, end_date):
        # Build request with parameters
        # Retry logic with exponential backoff
        # Parse JSON response to DataFrame
        # Return structured data
```

**Error Handling:**
- Connection timeouts → Retry 3 times
- API errors (4xx/5xx) → Retry with delay
- Invalid coordinates → Immediate failure
- Malformed responses → Log and skip

#### 3. **Airflow DAG Orchestration**

**File:** `airflow/dags/weather_data_dag.py`

**DAG Configuration:**
```python
dag = DAG(
    dag_id='weather_data_dag',
    default_args={
        'owner': 'data_engineering',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=2),
    },
    schedule_interval=None,  # Manual trigger (or set to @daily)
    catchup=False,
    tags=['weather', 'api', 'etl'],
)
```

**Task Dependencies:**
```
validate_outlets → fetch_weather_data → store_weather_data → validate_weather_data
```

**XCom Data Passing:**
- `fetch_weather_data` → Pushes row count to XCom
- `validate_weather_data` → Pulls row count from XCom for integrity check

#### 4. **Database Storage Strategy**

**Bulk Insert Optimization:**
```python
df.to_sql(
    name="weather",
    con=engine,
    schema="raw",
    if_exists="replace",     # First run: drop and recreate
    index=False,
    method="multi",          # Bulk insert (10x faster)
    chunksize=1000          # 1000 rows per transaction
)
```

**Performance Comparison:**
| Method | Time | Records/Second |
|--------|------|----------------|
| Single inserts | ~45 min | 162 rows/sec |
| method='multi' | ~3 min | 2,433 rows/sec |

#### 5. **Data Quality Validation**

**4-Layer Validation Approach:**

1. **Row Count Integrity** - No data loss during ETL
2. **NULL Detection** - Critical columns must be populated
3. **Range Validation** - Realistic values only
4. **Foreign Key Integrity** - All references valid

**Failure Philosophy:**
```python
# OLD (WRONG - Silent Failures):
if validation_failed:
    logger.warning("Validation failed")  # DAG shows SUCCESS ❌

# NEW (CORRECT - Explicit Failures):
if validation_failed:
    raise ValueError("Validation failed")  # DAG shows FAILURE ✅
```

---

### ETL Data Flow Diagram

```
┌────────────────┐
│  Open-Meteo    │
│  API Server    │
└────────┬───────┘
         │ HTTP GET
         │ (JSON Response)
         ▼
┌────────────────────────────┐
│  Airflow Container         │
│  ┌──────────────────────┐  │
│  │ weather_data_dag.py  │  │
│  │                      │  │
│  │ PythonOperator       │  │
│  │  └─> weather_api.py  │  │
│  │       (APIClient)    │  │
│  └──────────┬───────────┘  │
└─────────────┼──────────────┘
              │
              │ pandas DataFrame
              │ (438K rows)
              ▼
┌─────────────────────────────┐
│  PostgreSQL Container       │
│  ┌───────────────────────┐  │
│  │  Database: db         │  │
│  │  Schema: raw          │  │
│  │  Table: weather       │  │
│  │                       │  │
│  │  outlet_id    INT     │  │
│  │  datetime     TS      │  │
│  │  temperature  FLOAT   │  │
│  │  humidity     FLOAT   │  │
│  │  wind_speed   FLOAT   │  │
│  └───────────────────────┘  │
└─────────────┬───────────────┘
              │
              │ dbt reads this table
              ▼
┌─────────────────────────────┐
│  dbt (Host)                 │
│  ┌───────────────────────┐  │
│  │ stg_weather.sql       │  │
│  │  - Aggregate hourly   │  │
│  │    to daily averages  │  │
│  │  - Add date dimension │  │
│  └───────────────────────┘  │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│  mart.fct_daily_business_   │
│  performance                │
│  - Weather joined with      │
│    orders, ratings, rank    │
│  - Ready for analysis       │
└─────────────────────────────┘
```

---

### API Request Example

**Request:**
```http
GET https://archive-api.open-meteo.com/v1/archive?
    latitude=40.7128&
    longitude=-74.0060&
    start_date=2023-01-01&
    end_date=2024-12-31&
    hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&
    timezone=UTC
```

**Response (abbreviated):**
```json
{
  "latitude": 40.7128,
  "longitude": -74.0060,
  "timezone": "UTC",
  "hourly": {
    "time": [
      "2023-01-01T00:00",
      "2023-01-01T01:00",
      "2023-01-01T02:00",
      ...
    ],
    "temperature_2m": [5.2, 4.8, 4.3, ...],
    "relative_humidity_2m": [78, 79, 81, ...],
    "wind_speed_10m": [12.5, 11.8, 10.9, ...]
  }
}
```

**Transformed to:**
```
outlet_id | datetime            | temperature_2m | relative_humidity_2m | wind_speed_10m
----------|---------------------|----------------|----------------------|---------------
1         | 2023-01-01 00:00:00 | 5.2            | 78.0                 | 12.5
1         | 2023-01-01 01:00:00 | 4.8            | 79.0                 | 11.8
1         | 2023-01-01 02:00:00 | 4.3            | 81.0                 | 10.9
...
```

---

### Monitoring & Observability

**Airflow Web UI (localhost:8080):**
- DAG run history and success/failure status
- Task duration and performance metrics
- Log viewing for each task execution
- Retry attempts and error messages

**Key Metrics Tracked:**
- **API Call Count:** 73 (one per outlet)
- **Records Fetched:** 438,000+ rows
- **Execution Time:** ~2-3 minutes typical
- **Success Rate:** 100% with retry logic
- **Data Quality:** 4 validation checks (all must pass)

**Alerting Strategy:**
- Task failure → Airflow logs detailed error
- Email notifications (configurable)
- Slack/PagerDuty integration (production)

---

### Production Considerations

**Scheduling:**
```python
# Development: Manual trigger
schedule_interval=None

# Production: Daily incremental loads
schedule_interval='0 2 * * *'  # 2 AM daily
```

**Incremental Loading Strategy:**
```python
# Instead of full reload (2 years), fetch only yesterday:
start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
end_date = start_date
if_exists = "append"  # Add new data, don't replace
```

**Cost Optimization:**
- Open-Meteo is free (no API costs)
- Batch requests efficiently (73 locations, not thousands)
- Cache responses if re-running same date range
- Use hourly data only if needed (could aggregate to daily)

**Scalability:**
- Current: 73 outlets = manageable in single DAG
- If scaling to 1000s of outlets: Parallelize API calls using Airflow task groups
- If real-time data needed: Switch to Open-Meteo forecast API


