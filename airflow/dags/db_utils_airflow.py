"""
Database Utility Module for Airflow
Provides reusable functions for database connections within Airflow DAGs.

Note: This uses psycopg2 (not psycopg3) for Airflow compatibility.
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

    Note: Uses psycopg2 driver for Airflow compatibility.

    Args:
        user: Database username (defaults to POSTGRES_USER env var)
        password: Database password (defaults to POSTGRES_PASSWORD env var)
        host: Database host (defaults to 'postgres' for Docker)
        port: Database port (defaults to POSTGRES_PORT env var or 5432)
        database: Database name (defaults to POSTGRES_DB env var)

    Returns:
        PostgreSQL connection URL string

    Example:
        >>> url = get_database_url()
        >>> print(url)
        postgresql+psycopg2://dataeng:dataeng123@postgres:5432/business_db
    """
    # Use provided values or fall back to environment variables
    user = user or os.getenv('POSTGRES_USER', 'dataeng')
    password = password or os.getenv('POSTGRES_PASSWORD', 'dataeng123')
    # Use 'postgres' as default host for Docker (not 'localhost')
    host = host or os.getenv('POSTGRES_HOST', 'postgres')
    port = port or int(os.getenv('POSTGRES_PORT', '5432'))
    database = database or os.getenv('POSTGRES_DB', 'business_db')

    # Construct PostgreSQL URL using psycopg2 for Airflow
    database_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

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
