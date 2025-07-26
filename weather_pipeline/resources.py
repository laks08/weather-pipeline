"""
Dagster resources for the Boston Weather ETL Pipeline.
"""

import os
import duckdb
import requests
from dagster import resource
from typing import Dict, Any, Optional
import structlog

logger = structlog.get_logger()


@resource
def duckdb_resource(context):
    """DuckDB database resource."""
    db_path = "/data/weather.db"
    
    def get_connection():
        return duckdb.connect(db_path)
    
    def get_table_count(table_name: str) -> int:
        with get_connection() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            return result.fetchone()[0]
    
    def get_latest_weather() -> Optional[Dict[str, Any]]:
        """Get the most recent current weather record."""
        try:
            with get_connection() as conn:
                result = conn.execute(
                    "SELECT * FROM current_weather ORDER BY timestamp DESC LIMIT 1"
                )
                row = result.fetchone()
                if row:
                    columns = [desc[0] for desc in conn.description]
                    return dict(zip(columns, row))
                return None
        except Exception as e:
            logger.error(f"Failed to get latest weather: {e}")
            return None
    
    return {
        "get_connection": get_connection,
        "get_table_count": get_table_count,
        "get_latest_weather": get_latest_weather,
    }


@resource
def weather_api_resource(context):
    """National Weather Service API resource."""
    lat = float(os.getenv("BOSTON_LAT", "42.3601"))
    lon = float(os.getenv("BOSTON_LON", "-71.0589"))
    
    def fetch_weather_data() -> Optional[Dict[str, Any]]:
        """
        Note: This resource is deprecated. Weather data extraction is now handled
        by the WeatherExtractor class in the extractor module.
        """
        logger.warning("weather_api_resource is deprecated. Use WeatherExtractor instead.")
        return None
    
    def get_connection():
        """Get DuckDB connection for storing data."""
        return duckdb.connect("/data/weather.db")
    
    return {
        "fetch_weather_data": fetch_weather_data,
        "get_connection": get_connection,
    }


@resource
def dbt_resource(context):
    """DBT resource for running transformations."""
    
    def run_dbt_run() -> bool:
        import subprocess
        try:
            result = subprocess.run(
                ["dbt", "run"],
                cwd="/dbt",
                capture_output=True,
                text=True,
                check=True
            )
            logger.info("DBT run completed successfully")
            return True
        except Exception as e:
            logger.error(f"DBT run failed: {e}")
            return False
    
    return {
        "run_dbt_run": run_dbt_run,
    } 