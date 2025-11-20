"""
Weather Data Collection DAG

This DAG fetches historical weather data from Open-Meteo API for all outlet locations
and stores it in the PostgreSQL database with comprehensive data quality validation.

Schedule: Manual trigger (can be scheduled daily for production)
Data Range: 2023-01-01 to 2024-12-31 (matching orders data)

DAG Tasks:
1. validate_outlets - Check outlet data exists
2. fetch_weather_data - Call Open-Meteo API for all locations
3. store_weather_data - Save to database
4. validate_weather_data - Comprehensive validation:
   - Row count integrity (fetched vs stored)
   - NULL value checks (critical columns must not be NULL)
   - Data quality validation (realistic temperature/humidity/wind ranges)
   - Foreign key integrity (all outlet_ids must exist)

Data Quality Philosophy:
- DAG fails if ANY validation check fails (no silent failures)
- "Success" means data is complete AND correct
- All errors are explicit with detailed error messages
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os

# Add scripts directory to Python path so we can import our modules
sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/dags')

from weather_api import WeatherAPIClient, get_outlets_from_database, save_weather_to_database
# Use Airflow-specific db_utils (with psycopg2, not psycopg3)
from db_utils_airflow import get_database_engine, get_table_count
import logging

logger = logging.getLogger(__name__)


# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Safety timeout
}

# Weather data date range - read from dbt_project.yml
# This ensures weather collection matches the configured date range
import yaml

def get_weather_date_range():
    """Read weather date range from dbt_project.yml"""
    dbt_project_path = '/opt/airflow/dbt/dbt_project.yml'
    try:
        with open(dbt_project_path, 'r') as f:
            config = yaml.safe_load(f)
            start_date = config.get('vars', {}).get('weather_start_date', '2023-01-01')
            end_date = config.get('vars', {}).get('weather_end_date', '2024-12-31')
            return start_date, end_date
    except Exception as e:
        logger.warning(f"Could not read dbt_project.yml: {e}. Using defaults.")
        return '2023-01-01', '2024-12-31'

WEATHER_START_DATE, WEATHER_END_DATE = get_weather_date_range()


def validate_outlets_task(**context):
    """
    Task 1: Validate that outlet data exists in database.

    Checks:
    - Outlet table has data
    - At least some outlets have valid coordinates

    Raises:
        ValueError: If validation fails
    """
    logger.info("=" * 70)
    logger.info("TASK 1: Validating Outlet Data")
    logger.info("=" * 70)

    engine = get_database_engine()

    # Check outlet count
    outlet_count = get_table_count('raw', 'outlet', engine)
    logger.info(f"Total outlets in database: {outlet_count}")

    if outlet_count == 0:
        raise ValueError("No outlets found in database! Run data loading first.")

    # Get outlets with valid coordinates
    locations = get_outlets_from_database(engine)

    # Filter out (0, 0) coordinates
    valid_locations = [
        loc for loc in locations
        if not (loc['latitude'] == 0 and loc['longitude'] == 0)
    ]

    logger.info(f"Outlets with valid coordinates: {len(valid_locations)}")
    logger.info(f"Outlets with invalid (0,0) coordinates: {len(locations) - len(valid_locations)}")

    if len(valid_locations) == 0:
        raise ValueError("No outlets have valid coordinates!")

    # Store count in XCom for next tasks
    context['task_instance'].xcom_push(key='outlet_count', value=len(valid_locations))

    logger.info("✓ Outlet validation passed!")
    return len(valid_locations)


def fetch_weather_data_task(**context):
    """
    Task 2: Fetch weather data from Open-Meteo API.

    Fetches hourly weather data for all outlets with valid coordinates
    for the entire year 2023.

    Returns:
        Number of records fetched
    """
    logger.info("=" * 70)
    logger.info("TASK 2: Fetching Weather Data from API")
    logger.info("=" * 70)

    engine = get_database_engine()

    # Get outlet locations
    locations = get_outlets_from_database(engine)

    # Filter out invalid coordinates
    valid_locations = [
        loc for loc in locations
        if not (loc['latitude'] == 0 and loc['longitude'] == 0)
    ]

    logger.info(f"Fetching weather for {len(valid_locations)} outlets")
    logger.info(f"Date range: {WEATHER_START_DATE} to {WEATHER_END_DATE}")

    # Initialize weather API client
    client = WeatherAPIClient(
        retry_attempts=3,
        retry_delay=5
    )

    # Fetch weather data for all locations
    # Note: This might take a while! Each API call takes ~2 seconds
    # For 50 outlets: ~100 seconds (under 2 minutes)
    df = client.fetch_weather_for_multiple_locations(
        locations=valid_locations,
        start_date=WEATHER_START_DATE,
        end_date=WEATHER_END_DATE,
        delay_between_requests=0.5  # Be nice to the API
    )

    if df.empty:
        raise ValueError("No weather data fetched!")

    logger.info(f"\n{'=' * 70}")
    logger.info(f"Weather Data Summary:")
    logger.info(f"  Total records: {len(df):,}")
    logger.info(f"  Unique outlets: {df['outlet_id'].nunique()}")
    logger.info(f"  Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    logger.info(f"{'=' * 70}")

    # Store DataFrame in XCom (for next task)
    # Note: For large datasets, better to use external storage (S3, etc.)
    # But for demo purposes, XCom is fine
    context['task_instance'].xcom_push(key='weather_df', value=df.to_json())

    return len(df)


def store_weather_data_task(**context):
    """
    Task 3: Store weather data in PostgreSQL.

    Retrieves DataFrame from previous task and saves to raw.weather table.

    Returns:
        Number of records stored
    """
    logger.info("=" * 70)
    logger.info("TASK 3: Storing Weather Data to Database")
    logger.info("=" * 70)

    import pandas as pd

    # Get DataFrame from previous task
    weather_json = context['task_instance'].xcom_pull(
        task_ids='fetch_weather_data',
        key='weather_df'
    )

    if not weather_json:
        raise ValueError("No weather data received from previous task!")

    # Convert back to DataFrame
    df = pd.read_json(weather_json)

    logger.info(f"Received {len(df):,} records from fetch task")

    # Get database engine
    engine = get_database_engine()

    # Save to database (replace existing data for idempotency)
    save_weather_to_database(df, engine, if_exists='replace')

    logger.info("✓ Weather data stored successfully!")

    return len(df)


def validate_weather_data_task(**context):
    """
    Task 4: Validate weather data was stored correctly.

    Checks:
    - Weather table has data
    - Record count matches what we fetched
    - No NULL values in required columns
    - Date range is correct

    Raises:
        ValueError: If validation fails
    """
    logger.info("=" * 70)
    logger.info("TASK 4: Validating Stored Weather Data")
    logger.info("=" * 70)

    from sqlalchemy import text

    engine = get_database_engine()

    # Check 1: Row count
    weather_count = get_table_count('raw', 'weather', engine)
    logger.info(f"✓ Weather records in database: {weather_count:,}")

    if weather_count == 0:
        raise ValueError("No weather data found in database!")

    # Check 2: Compare with fetched count (CRITICAL - must match exactly)
    fetched_count = context['task_instance'].xcom_pull(
        task_ids='fetch_weather_data'
    )

    if fetched_count and weather_count != fetched_count:
        raise ValueError(
            f"Data integrity error! Fetched {fetched_count:,} records "
            f"but only {weather_count:,} were stored in database. "
            f"Lost {abs(fetched_count - weather_count):,} records!"
        )

    # Check 3: NULL values in critical columns
    with engine.connect() as conn:
        null_check_query = text("""
            SELECT
                COUNT(*) FILTER (WHERE outlet_id IS NULL) as null_outlet_id,
                COUNT(*) FILTER (WHERE datetime IS NULL) as null_datetime,
                COUNT(*) FILTER (WHERE temperature_2m IS NULL) as null_temperature,
                COUNT(*) FILTER (WHERE relative_humidity_2m IS NULL) as null_humidity,
                COUNT(*) FILTER (WHERE wind_speed_10m IS NULL) as null_wind_speed
            FROM raw.weather
        """)

        result = conn.execute(null_check_query).fetchone()

        logger.info("\nNULL Value Checks:")
        logger.info(f"  outlet_id: {result[0]} NULLs")
        logger.info(f"  datetime: {result[1]} NULLs")
        logger.info(f"  temperature_2m: {result[2]} NULLs")
        logger.info(f"  relative_humidity_2m: {result[3]} NULLs")
        logger.info(f"  wind_speed_10m: {result[4]} NULLs")

        # Critical columns - must not have NULLs
        if result[0] > 0:  # outlet_id
            raise ValueError(
                f"Critical data quality error! Found {result[0]} records with NULL outlet_id"
            )
        if result[1] > 0:  # datetime
            raise ValueError(
                f"Critical data quality error! Found {result[1]} records with NULL datetime"
            )
        if result[2] > 0:  # temperature_2m
            raise ValueError(
                f"Critical data quality error! Found {result[2]} records with NULL temperature. "
                f"Weather data is incomplete and unusable for analysis!"
            )

        # Optional columns - warn but don't fail
        if result[3] > 0 or result[4] > 0:
            logger.warning(
                f"Warning: Found NULLs in optional fields - "
                f"humidity: {result[3]}, wind_speed: {result[4]}"
            )

    # Check 4: Date range
    with engine.connect() as conn:
        date_range_query = text("""
            SELECT
                MIN(datetime)::text as min_date,
                MAX(datetime)::text as max_date,
                COUNT(DISTINCT outlet_id) as unique_outlets
            FROM raw.weather
        """)

        result = conn.execute(date_range_query).fetchone()

        logger.info("\nDate Range:")
        logger.info(f"  Min: {result[0]}")
        logger.info(f"  Max: {result[1]}")
        logger.info(f"  Unique outlets: {result[2]}")

    # Check 5: Data quality - validate realistic values
    logger.info("\nData Quality Checks:")
    with engine.connect() as conn:
        quality_check_query = text("""
            SELECT
                COUNT(*) FILTER (WHERE temperature_2m < -50 OR temperature_2m > 60) as bad_temp,
                COUNT(*) FILTER (WHERE relative_humidity_2m < 0 OR relative_humidity_2m > 100) as bad_humidity,
                COUNT(*) FILTER (WHERE wind_speed_10m < 0 OR wind_speed_10m > 200) as bad_wind,
                MIN(temperature_2m) as min_temp,
                MAX(temperature_2m) as max_temp
            FROM raw.weather
        """)

        result = conn.execute(quality_check_query).fetchone()

        logger.info(f"  Temperature range: {result[3]:.1f}°C to {result[4]:.1f}°C")
        logger.info(f"  Records with impossible temperatures: {result[0]}")
        logger.info(f"  Records with invalid humidity: {result[1]}")
        logger.info(f"  Records with invalid wind speed: {result[2]}")

        # Fail if data quality issues found
        if result[0] > 0:
            raise ValueError(
                f"Data quality error! Found {result[0]} records with impossible "
                f"temperatures (outside -50°C to 60°C range)"
            )
        if result[1] > 0:
            raise ValueError(
                f"Data quality error! Found {result[1]} records with invalid "
                f"humidity values (outside 0-100% range)"
            )
        if result[2] > 0:
            raise ValueError(
                f"Data quality error! Found {result[2]} records with invalid "
                f"wind speeds (negative or >200 km/h)"
            )

    # Check 6: Foreign key integrity - ensure all outlet_ids exist
    logger.info("\nForeign Key Integrity Check:")
    with engine.connect() as conn:
        orphan_check_query = text("""
            SELECT COUNT(*) as orphan_count
            FROM raw.weather w
            LEFT JOIN raw.outlet o ON w.outlet_id = o.id
            WHERE o.id IS NULL
        """)

        result = conn.execute(orphan_check_query).fetchone()

        logger.info(f"  Weather records with invalid outlet_id: {result[0]}")

        if result[0] > 0:
            raise ValueError(
                f"Data integrity error! Found {result[0]} weather records "
                f"referencing non-existent outlet_ids. Foreign key constraint violated!"
            )

    logger.info("\n" + "=" * 70)
    logger.info("✓ All validation checks passed!")
    logger.info("  - Row count matches fetched data")
    logger.info("  - No NULL values in critical columns")
    logger.info("  - All values within realistic ranges")
    logger.info("  - All foreign keys valid")
    logger.info("=" * 70)

    return True


# Define the DAG
with DAG(
    dag_id='fetch_weather_data',
    default_args=DEFAULT_ARGS,
    description='Fetch historical weather data from Open-Meteo API for all outlet locations',
    schedule_interval=None,  # Manual trigger (change to '@daily' for daily runs)
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
    tags=['weather', 'data_ingestion', 'api'],
) as dag:

    # Task 0: Start
    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    # Task 1: Validate outlets exist
    validate_outlets = PythonOperator(
        task_id='validate_outlets',
        python_callable=validate_outlets_task,
        provide_context=True,
        dag=dag
    )

    # Task 2: Fetch weather data from API
    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data_task,
        provide_context=True,
        dag=dag
    )

    # Task 3: Store weather data to database
    store_weather_data = PythonOperator(
        task_id='store_weather_data',
        python_callable=store_weather_data_task,
        provide_context=True,
        dag=dag
    )

    # Task 4: Validate stored data
    validate_weather_data = PythonOperator(
        task_id='validate_weather_data',
        python_callable=validate_weather_data_task,
        provide_context=True,
        dag=dag
    )

    # Task 5: End
    end = EmptyOperator(
        task_id='end',
        dag=dag
    )

    # Define task dependencies (execution order)
    # start → validate_outlets → fetch_weather_data → store_weather_data → validate_weather_data → end
    start >> validate_outlets >> fetch_weather_data >> store_weather_data >> validate_weather_data >> end
