"""
CSV Data Loading Script
Loads all CSV files from the data/ directory into PostgreSQL raw schema.

This script:
1. Reads CSV files using pandas
2. Handles data type conversions
3. Loads data into raw schema tables
4. Provides detailed logging and error handling
5. Generates a summary report

Usage:
    python scripts/load_csv_data.py [--truncate]

Options:
    --truncate: Clear existing data before loading (default: append)
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
import argparse

# Add scripts directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_utils import get_database_engine, get_table_count, truncate_table, test_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data_loading.log')
    ]
)
logger = logging.getLogger(__name__)


# CSV file to table mapping
# Format: {'csv_filename': 'table_name'}
CSV_TABLE_MAPPING = {
    'listing.csv': 'listing',
    'orders.csv': 'orders',
    'orders_daily.csv': 'orders_daily',
    'org.csv': 'org',
    'outlet.csv': 'outlet',
    'platform.csv': 'platform',
    'rank.csv': 'rank',
    'ratings_agg.csv': 'ratings_agg'
}

# Data type specifications for each table
# This ensures consistent data types in PostgreSQL
DTYPE_MAPPING = {
    'listing': {
        'id': 'Int64',
        'outlet_id': 'Int64',
        'platform_id': 'Int64'
    },
    'orders': {
        'listing_id': 'Int64',
        'order_id': 'Int64',
        'status': 'string'
    },
    'orders_daily': {
        'listing_id': 'Int64',
        'orders': 'Int64'
    },
    'org': {
        'id': 'Int64',
        'name': 'string'
    },
    'outlet': {
        'id': 'Int64',
        'org_id': 'Int64',
        'name': 'string',
        'latitude': 'float64',
        'longitude': 'float64'
    },
    'platform': {
        'id': 'Int64',
        'group': 'string',
        'name': 'string',
        'country': 'string'
    },
    'rank': {
        'listing_id': 'Int64',
        'is_online': 'boolean',
        'rank': 'float64'
    },
    'ratings_agg': {
        'listing_id': 'Int64',
        'cnt_ratings': 'Int64',
        'avg_rating': 'float64'
    }
}


def get_data_directory() -> Path:
    """
    Returns the path to the data directory.
    Works regardless of where script is run from.
    """
    # Get project root (parent of scripts directory)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    data_dir = project_root / 'data'

    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    return data_dir


def read_csv_file(csv_path: Path, table_name: str) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame with appropriate data types.

    Args:
        csv_path: Path to CSV file
        table_name: Name of target table (for dtype mapping)

    Returns:
        pandas DataFrame with data

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        pd.errors.EmptyDataError: If CSV is empty
    """
    logger.info(f"Reading {csv_path.name}...")

    try:
        # Read CSV with specified dtypes if available
        dtype = DTYPE_MAPPING.get(table_name, None)

        df = pd.read_csv(
            csv_path,
            dtype=dtype,
            parse_dates=True  # Auto-detect date columns
        )

        # Manually parse timestamp columns
        # Pandas sometimes doesn't auto-detect them
        timestamp_cols = ['timestamp', 'placed_at', 'datetime']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Parse date columns
        date_cols = ['date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        logger.info(f"  ✓ Read {len(df):,} rows, {len(df.columns)} columns")

        return df

    except FileNotFoundError:
        logger.error(f"  ✗ File not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.warning(f"  ! CSV file is empty: {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"  ✗ Error reading {csv_path}: {e}")
        raise


def load_dataframe_to_db(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    engine,
    if_exists: str = 'append'
) -> int:
    """
    Loads a pandas DataFrame into a PostgreSQL table.

    Args:
        df: DataFrame to load
        table_name: Target table name
        schema: Target schema name
        engine: SQLAlchemy engine
        if_exists: What to do if table exists ('append', 'replace', 'fail')

    Returns:
        Number of rows loaded

    Raises:
        Exception: If loading fails
    """
    if df.empty:
        logger.warning(f"  ! DataFrame is empty, skipping load to {schema}.{table_name}")
        return 0

    try:
        logger.info(f"Loading data into {schema}.{table_name}...")

        # Load to database
        # method='multi' inserts multiple rows at once (faster)
        # chunksize splits large DataFrames into chunks
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,  # Don't write DataFrame index
            method='multi',
            chunksize=1000
        )

        rows_loaded = len(df)
        logger.info(f"  ✓ Loaded {rows_loaded:,} rows to {schema}.{table_name}")

        return rows_loaded

    except Exception as e:
        logger.error(f"  ✗ Error loading to {schema}.{table_name}: {e}")
        raise


def load_all_csv_files(truncate: bool = False) -> Dict[str, Tuple[int, int]]:
    """
    Loads all CSV files into the raw schema.

    Args:
        truncate: If True, truncate tables before loading

    Returns:
        Dictionary mapping table names to (rows_loaded, total_rows) tuples

    Example:
        >>> results = load_all_csv_files()
        >>> print(results)
        {'listing': (200, 200), 'orders': (5000, 5000), ...}
    """
    logger.info("=" * 70)
    logger.info("CSV Data Loading - Starting")
    logger.info("=" * 70)

    # Get database engine
    engine = get_database_engine()

    # Test connection
    if not test_connection(engine):
        logger.error("Cannot connect to database. Exiting.")
        sys.exit(1)

    # Get data directory
    data_dir = get_data_directory()
    logger.info(f"Data directory: {data_dir}")

    # Results tracking
    results = {}
    total_files = len(CSV_TABLE_MAPPING)
    success_count = 0
    error_count = 0

    # Load each CSV file
    for idx, (csv_filename, table_name) in enumerate(CSV_TABLE_MAPPING.items(), 1):
        logger.info(f"\n[{idx}/{total_files}] Processing {csv_filename}")

        try:
            # Build CSV path
            csv_path = data_dir / csv_filename

            # Check file exists
            if not csv_path.exists():
                logger.warning(f"  ! CSV file not found: {csv_path}")
                results[table_name] = (0, 0)
                error_count += 1
                continue

            # Truncate if requested
            if truncate:
                truncate_table('raw', table_name, engine)

            # Read CSV
            df = read_csv_file(csv_path, table_name)

            # Load to database
            rows_loaded = load_dataframe_to_db(
                df=df,
                table_name=table_name,
                schema='raw',
                engine=engine,
                if_exists='append'
            )

            # Get total count in database
            total_rows = get_table_count('raw', table_name, engine)

            # Store results
            results[table_name] = (rows_loaded, total_rows)
            success_count += 1

        except Exception as e:
            logger.error(f"  ✗ Failed to load {csv_filename}: {e}")
            results[table_name] = (0, 0)
            error_count += 1

    # Print summary
    print_summary(results, success_count, error_count)

    return results


def print_summary(
    results: Dict[str, Tuple[int, int]],
    success_count: int,
    error_count: int
):
    """
    Prints a formatted summary of the data loading process.

    Args:
        results: Dictionary of table results
        success_count: Number of successful loads
        error_count: Number of failed loads
    """
    logger.info("\n" + "=" * 70)
    logger.info("DATA LOADING SUMMARY")
    logger.info("=" * 70)

    # Table header
    logger.info(f"\n{'Table Name':<20} {'Rows Loaded':>15} {'Total in DB':>15}")
    logger.info("-" * 70)

    # Table rows
    total_loaded = 0
    total_in_db = 0

    for table_name, (loaded, total) in sorted(results.items()):
        logger.info(f"{table_name:<20} {loaded:>15,} {total:>15,}")
        total_loaded += loaded
        total_in_db += total

    # Footer
    logger.info("-" * 70)
    logger.info(f"{'TOTAL':<20} {total_loaded:>15,} {total_in_db:>15,}")
    logger.info("-" * 70)

    # Status
    logger.info(f"\nSuccessful: {success_count}/{len(results)}")
    logger.info(f"Failed: {error_count}/{len(results)}")

    if error_count == 0:
        logger.info("\n✓ All CSV files loaded successfully!")
    else:
        logger.warning(f"\n! {error_count} file(s) failed to load")

    logger.info("=" * 70)


def main():
    """
    Main entry point for the script.
    Handles command-line arguments and executes loading.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Load CSV files into PostgreSQL raw schema'
    )
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate tables before loading (default: append)'
    )

    args = parser.parse_args()

    # Load data
    try:
        results = load_all_csv_files(truncate=args.truncate)

        # Exit code based on results
        if all(loaded > 0 or total == 0 for loaded, total in results.values()):
            sys.exit(0)  # Success
        else:
            sys.exit(1)  # Some failures

    except KeyboardInterrupt:
        logger.warning("\n\nLoading interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
