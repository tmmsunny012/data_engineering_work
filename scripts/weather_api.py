"""
Weather API Module
Handles interactions with Open-Meteo API for historical weather data.

Open-Meteo API Documentation:
https://open-meteo.com/en/docs/historical-weather-api
"""

import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Open-Meteo API configuration
WEATHER_API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_METRICS = [
    "temperature_2m",        # Temperature at 2 meters height (°C)
    "relative_humidity_2m",  # Relative humidity at 2 meters height (%)
    "wind_speed_10m"         # Wind speed at 10 meters height (km/h)
]


class WeatherAPIClient:
    """
    Client for fetching historical weather data from Open-Meteo API.

    Example usage:
        >>> client = WeatherAPIClient()
        >>> df = client.fetch_weather_data(
        ...     latitude=40.7128,
        ...     longitude=-74.0060,
        ...     start_date="2023-01-01",
        ...     end_date="2023-01-31"
        ... )
        >>> print(df.head())
    """

    def __init__(
        self,
        base_url: str = WEATHER_API_BASE_URL,
        metrics: List[str] = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        retry_delay: int = 5
    ):
        """
        Initialize Weather API client.

        Args:
            base_url: API endpoint URL
            metrics: List of weather metrics to fetch
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts on failure
            retry_delay: Delay between retries in seconds
        """
        self.base_url = base_url
        self.metrics = metrics or WEATHER_METRICS
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

    def fetch_weather_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        timezone: str = "UTC"
    ) -> pd.DataFrame:
        """
        Fetch hourly weather data for a location and date range.

        Args:
            latitude: Location latitude (-90 to 90)
            longitude: Location longitude (-180 to 180)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            timezone: Timezone for timestamps (default: UTC)

        Returns:
            DataFrame with columns: datetime, temperature_2m, relative_humidity_2m, wind_speed_10m

        Raises:
            ValueError: If coordinates are invalid
            requests.RequestException: If API request fails
        """
        # Validate coordinates
        self._validate_coordinates(latitude, longitude)

        # Build request parameters
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(self.metrics),
            "timezone": timezone
        }

        logger.info(
            f"Fetching weather data for ({latitude}, {longitude}) "
            f"from {start_date} to {end_date}"
        )

        # Make API request with retries
        response_data = self._make_request_with_retry(params)

        # Parse response into DataFrame
        df = self._parse_response(response_data)

        logger.info(f"  ✓ Fetched {len(df):,} hourly records")

        return df

    def fetch_weather_for_multiple_locations(
        self,
        locations: List[Dict],
        start_date: str,
        end_date: str,
        delay_between_requests: float = 0.5
    ) -> pd.DataFrame:
        """
        Fetch weather data for multiple locations.

        Args:
            locations: List of dicts with keys: outlet_id, latitude, longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            delay_between_requests: Delay between API calls (seconds) to respect rate limits

        Returns:
            Combined DataFrame with outlet_id column

        Example:
            >>> locations = [
            ...     {"outlet_id": 1, "latitude": 40.7128, "longitude": -74.0060},
            ...     {"outlet_id": 2, "latitude": 34.0522, "longitude": -118.2437}
            ... ]
            >>> df = client.fetch_weather_for_multiple_locations(
            ...     locations, "2023-01-01", "2023-01-31"
            ... )
        """
        all_data = []
        total_locations = len(locations)

        logger.info(f"Fetching weather for {total_locations} locations...")

        for idx, location in enumerate(locations, 1):
            outlet_id = location["outlet_id"]
            latitude = location["latitude"]
            longitude = location["longitude"]

            # Skip invalid coordinates (0, 0)
            if latitude == 0 and longitude == 0:
                logger.warning(
                    f"  [{idx}/{total_locations}] Skipping outlet {outlet_id}: "
                    f"Invalid coordinates (0, 0)"
                )
                continue

            logger.info(f"  [{idx}/{total_locations}] Outlet {outlet_id}...")

            try:
                # Fetch weather data
                df = self.fetch_weather_data(
                    latitude=latitude,
                    longitude=longitude,
                    start_date=start_date,
                    end_date=end_date
                )

                # Add outlet_id column
                df.insert(0, "outlet_id", outlet_id)

                all_data.append(df)

                # Delay to respect rate limits
                if idx < total_locations:
                    time.sleep(delay_between_requests)

            except Exception as e:
                logger.error(
                    f"  ✗ Failed to fetch weather for outlet {outlet_id}: {e}"
                )
                continue

        if not all_data:
            logger.warning("No weather data fetched for any location!")
            return pd.DataFrame()

        # Combine all DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)

        logger.info(f"\n✓ Total records fetched: {len(combined_df):,}")

        return combined_df

    def _validate_coordinates(self, latitude: float, longitude: float):
        """Validate latitude and longitude ranges."""
        if not (-90 <= latitude <= 90):
            raise ValueError(f"Invalid latitude: {latitude}. Must be between -90 and 90.")

        if not (-180 <= longitude <= 180):
            raise ValueError(f"Invalid longitude: {longitude}. Must be between -180 and 180.")

    def _make_request_with_retry(self, params: Dict) -> Dict:
        """
        Make API request with retry logic.

        Args:
            params: Request parameters

        Returns:
            Response JSON as dictionary

        Raises:
            requests.RequestException: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout
                )

                # Raise exception for HTTP errors
                response.raise_for_status()

                return response.json()

            except requests.RequestException as e:
                last_exception = e

                if attempt < self.retry_attempts:
                    logger.warning(
                        f"  ! Request failed (attempt {attempt}/{self.retry_attempts}): {e}"
                    )
                    logger.warning(f"  ! Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"  ✗ Request failed after {self.retry_attempts} attempts"
                    )

        raise last_exception

    def _parse_response(self, response_data: Dict) -> pd.DataFrame:
        """
        Parse API response into pandas DataFrame.

        Args:
            response_data: JSON response from API

        Returns:
            DataFrame with datetime and weather metrics
        """
        hourly_data = response_data.get("hourly", {})

        # Extract timestamps
        timestamps = hourly_data.get("time", [])

        # Build DataFrame
        df_data = {
            "datetime": pd.to_datetime(timestamps)
        }

        # Add each metric
        for metric in self.metrics:
            df_data[metric] = hourly_data.get(metric, [])

        df = pd.DataFrame(df_data)

        return df


def get_outlets_from_database(engine) -> List[Dict]:
    """
    Fetch outlet locations from database.

    Args:
        engine: SQLAlchemy engine

    Returns:
        List of dicts with outlet_id, latitude, longitude
    """
    from sqlalchemy import text

    query = text("""
        SELECT
            id as outlet_id,
            latitude,
            longitude
        FROM raw.outlet
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY id
    """)

    with engine.connect() as conn:
        result = conn.execute(query)

        locations = [
            {
                "outlet_id": row[0],
                "latitude": float(row[1]),
                "longitude": float(row[2])
            }
            for row in result
        ]

    logger.info(f"Found {len(locations)} outlets with coordinates")

    return locations


def save_weather_to_database(df: pd.DataFrame, engine, if_exists: str = "append"):
    """
    Save weather data to database.

    Args:
        df: DataFrame with weather data
        engine: SQLAlchemy engine
        if_exists: What to do if table exists ('append', 'replace')
    """
    if df.empty:
        logger.warning("No weather data to save")
        return

    logger.info(f"Saving {len(df):,} weather records to database...")

    # If replacing, truncate the table instead of dropping it
    # This preserves dependent views/models
    if if_exists == "replace":
        try:
            from sqlalchemy import text
            with engine.begin() as conn:  # .begin() auto-commits on success
                conn.execute(text("TRUNCATE TABLE raw.weather"))
            logger.info("  ✓ Existing weather data truncated")
            if_exists = "append"  # Now append to the empty table
        except Exception as e:
            logger.warning(f"  Could not truncate table (might not exist yet): {e}")
            if_exists = "append"  # Try append anyway

    # Save to raw.weather table
    df.to_sql(
        name="weather",
        con=engine,
        schema="raw",
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000
    )

    logger.info("  ✓ Weather data saved successfully")


# Example usage and testing
if __name__ == "__main__":
    """
    Test the weather API client.
    Usage: python scripts/weather_api.py
    """
    print("=" * 70)
    print("Weather API Client - Test")
    print("=" * 70)

    # Test 1: Fetch weather for a single location
    print("\nTest 1: Fetching weather for New York City")
    print("-" * 70)

    client = WeatherAPIClient()

    try:
        df = client.fetch_weather_data(
            latitude=40.7128,
            longitude=-74.0060,
            start_date="2023-01-01",
            end_date="2023-01-07"  # Just one week for testing
        )

        print(f"\nFetched {len(df):,} records")
        print(f"\nSample data:")
        print(df.head(10))

        print(f"\nData summary:")
        print(df.describe())

    except Exception as e:
        print(f"\n✗ Error: {e}")

    print("\n" + "=" * 70)
    print("Test completed!")
    print("=" * 70)
