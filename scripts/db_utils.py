"""
Database Utility Module
Provides reusable functions for database connections and operations.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_url(
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None
) -> str:
    """
    Constructs PostgreSQL database URL from parameters or environment variables.

    Args:
        user: Database username (defaults to POSTGRES_USER env var)
        password: Database password (defaults to POSTGRES_PASSWORD env var)
        host: Database host (defaults to 'localhost')
        port: Database port (defaults to POSTGRES_PORT env var or 5432)
        database: Database name (defaults to POSTGRES_DB env var)

    Returns:
        PostgreSQL connection URL string

    Example:
        >>> url = get_database_url()
        >>> print(url)
        postgresql://dataeng:dataeng123@localhost:5432/business_db
    """
    # Use provided values or fall back to environment variables
    user = user or os.getenv('POSTGRES_USER', 'dataeng')
    password = password or os.getenv('POSTGRES_PASSWORD', 'dataeng123')
    host = host or os.getenv('POSTGRES_HOST', 'localhost')
    port = port or int(os.getenv('POSTGRES_PORT', '5432'))
    database = database or os.getenv('POSTGRES_DB', 'business_db')

    # Construct PostgreSQL URL
    # Using postgresql+psycopg for psycopg3 driver (Python 3.13 compatible)
    database_url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"

    return database_url


def get_database_engine(database_url: Optional[str] = None) -> Engine:
    """
    Creates and returns a SQLAlchemy engine for database connections.

    Args:
        database_url: Database connection URL (if None, constructs from env vars)

    Returns:
        SQLAlchemy Engine object

    Example:
        >>> engine = get_database_engine()
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    if database_url is None:
        database_url = get_database_url()

    # Create engine with connection pooling
    engine = create_engine(
        database_url,
        pool_pre_ping=True,  # Verify connections before using
        pool_size=5,         # Number of permanent connections
        max_overflow=10,     # Additional connections when pool is full
        echo=False           # Don't log all SQL (change to True for debugging)
    )

    logger.info(f"Database engine created for: {database_url.split('@')[1]}")

    return engine


def test_connection(engine: Optional[Engine] = None) -> bool:
    """
    Tests database connectivity.

    Args:
        engine: SQLAlchemy engine (if None, creates new one)

    Returns:
        True if connection successful, False otherwise

    Example:
        >>> if test_connection():
        ...     print("Database is reachable!")
    """
    if engine is None:
        engine = get_database_engine()

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            conn.commit()
        logger.info("✓ Database connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def get_table_count(schema: str, table: str, engine: Optional[Engine] = None) -> int:
    """
    Returns the number of rows in a table.

    Args:
        schema: Schema name (e.g., 'raw', 'staging')
        table: Table name (e.g., 'listing')
        engine: SQLAlchemy engine (if None, creates new one)

    Returns:
        Number of rows in the table

    Example:
        >>> count = get_table_count('raw', 'listing')
        >>> print(f"Table has {count} rows")
    """
    if engine is None:
        engine = get_database_engine()

    try:
        query = text(f"SELECT COUNT(*) FROM {schema}.{table}")
        with engine.connect() as conn:
            result = conn.execute(query)
            count = result.scalar()
        return count
    except Exception as e:
        logger.error(f"Error counting rows in {schema}.{table}: {e}")
        return 0


def truncate_table(schema: str, table: str, engine: Optional[Engine] = None) -> bool:
    """
    Truncates (empties) a table.

    Args:
        schema: Schema name
        table: Table name
        engine: SQLAlchemy engine

    Returns:
        True if successful, False otherwise

    Warning:
        This deletes all data in the table!
    """
    if engine is None:
        engine = get_database_engine()

    try:
        query = text(f"TRUNCATE TABLE {schema}.{table}")
        with engine.connect() as conn:
            conn.execute(query)
            conn.commit()
        logger.info(f"✓ Truncated {schema}.{table}")
        return True
    except Exception as e:
        logger.error(f"✗ Error truncating {schema}.{table}: {e}")
        return False


if __name__ == "__main__":
    """
    Test the database utilities when run directly.
    Usage: python scripts/db_utils.py
    """
    print("=" * 60)
    print("Database Utilities - Connection Test")
    print("=" * 60)

    # Test 1: Get database URL
    print("\n1. Database URL:")
    url = get_database_url()
    # Hide password in output
    safe_url = url.replace(os.getenv('POSTGRES_PASSWORD', 'dataeng123'), '****')
    print(f"   {safe_url}")

    # Test 2: Create engine
    print("\n2. Creating database engine...")
    engine = get_database_engine()
    print(f"   ✓ Engine created")

    # Test 3: Test connection
    print("\n3. Testing connection...")
    if test_connection(engine):
        print("   ✓ Connection successful!")
    else:
        print("   ✗ Connection failed!")
        exit(1)

    # Test 4: Check schemas exist
    print("\n4. Checking schemas...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN ('raw', 'staging', 'intermediate', 'mart')
                ORDER BY schema_name
            """))
            schemas = [row[0] for row in result]
            for schema in schemas:
                print(f"   ✓ Schema '{schema}' exists")
    except Exception as e:
        print(f"   ✗ Error checking schemas: {e}")

    print("\n" + "=" * 60)
    print("Database connection test completed!")
    print("=" * 60)
