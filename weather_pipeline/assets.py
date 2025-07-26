"""
Dagster assets for the Boston Weather ETL Pipeline.
"""

import os
import sys
import subprocess
from datetime import datetime
from typing import Dict, Any, Optional

from dagster import asset, AssetExecutionContext, AssetMaterialization, MetadataValue
import structlog

# Add the extractor to the path
sys.path.append('/app/extractor')

logger = structlog.get_logger()


@asset(
    description="Extract all weather data from National Weather Service API",
    group_name="raw_data"
)
def raw_weather_data(context: AssetExecutionContext) -> Dict[str, Any]:
    """Extract all weather data from National Weather Service API in a single operation."""
    try:
        # Import the extractor components
        from extractor.main import WeatherExtractor
        
        # Create extractor and run data extraction once
        extractor = WeatherExtractor()
        extractor.extract_and_store_weather_data()
        
        # Get metadata from all tables
        with extractor.db_manager(extractor.db_path) as db:
            # Get latest current weather
            latest_weather = db.get_latest_current_weather()
            
            # Get record counts
            current_count = db.connection.execute("SELECT COUNT(*) FROM current_weather").fetchone()[0]
            hourly_count = db.connection.execute("SELECT COUNT(*) FROM hourly_weather WHERE timestamp::date = current_date").fetchone()[0]
            daily_count = db.connection.execute("SELECT COUNT(*) FROM daily_weather WHERE date >= current_date").fetchone()[0]
        
        metadata = {
            "current_records": MetadataValue.int(current_count),
            "hourly_records_today": MetadataValue.int(hourly_count),
            "daily_forecast_records": MetadataValue.int(daily_count),
            "total_records": MetadataValue.int(current_count + hourly_count + daily_count)
        }
        
        if latest_weather:
            metadata.update({
                "temperature": MetadataValue.float(latest_weather.get('temp', 0)),
                "humidity": MetadataValue.int(latest_weather.get('humidity', 0)),
                "description": MetadataValue.text(latest_weather.get('description', 'N/A')),
                "timestamp": MetadataValue.text(str(latest_weather.get('timestamp', 'N/A')))
            })
        
        context.add_output_metadata(metadata)
        
        return {
            "status": "success", 
            "timestamp": datetime.now().isoformat(),
            "records": {
                "current": current_count,
                "hourly": hourly_count,
                "daily": daily_count
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to extract weather data: {e}")
        context.add_output_metadata({
            "error": MetadataValue.text(str(e)),
            "status": MetadataValue.text("failed")
        })
        raise


# Create separate assets that depend on the main extraction for better pipeline visualization
@asset(
    description="Current weather data extracted from NWS API",
    deps=[raw_weather_data],
    group_name="raw_data"
)
def raw_current_weather(context: AssetExecutionContext) -> Dict[str, Any]:
    """Current weather data asset - references data extracted by raw_weather_data."""
    # Just return success since the actual extraction is done by raw_weather_data
    context.add_output_metadata({
        "data_source": MetadataValue.text("raw_weather_data"),
        "table": MetadataValue.text("current_weather"),
        "status": MetadataValue.text("success")
    })
    
    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "table": "current_weather"
    }


@asset(
    description="Hourly weather forecast data extracted from NWS API", 
    deps=[raw_weather_data],
    group_name="raw_data"
)
def raw_hourly_weather(context: AssetExecutionContext) -> Dict[str, Any]:
    """Hourly weather forecast data asset - references data extracted by raw_weather_data."""
    # Just return success since the actual extraction is done by raw_weather_data
    context.add_output_metadata({
        "data_source": MetadataValue.text("raw_weather_data"),
        "table": MetadataValue.text("hourly_weather"),
        "status": MetadataValue.text("success")
    })
    
    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "table": "hourly_weather"
    }


@asset(
    description="Daily weather forecast data extracted from NWS API",
    deps=[raw_weather_data], 
    group_name="raw_data"
)
def raw_daily_weather(context: AssetExecutionContext) -> Dict[str, Any]:
    """Daily weather forecast data asset - references data extracted by raw_weather_data."""
    # Just return success since the actual extraction is done by raw_weather_data
    context.add_output_metadata({
        "data_source": MetadataValue.text("raw_weather_data"),
        "table": MetadataValue.text("daily_weather"),
        "status": MetadataValue.text("success")
    })
    
    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "table": "daily_weather"
    }


@asset(
    description="Run DBT transformations to create staging models",
    deps=[raw_current_weather, raw_hourly_weather, raw_daily_weather],
    group_name="staging"
)
def dbt_staging_models(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run DBT staging models."""
    try:
        # Run DBT staging models
        result = subprocess.run(
            ["dbt", "run", "--select", "staging"],
            cwd="/dbt",
            capture_output=True,
            text=True,
            check=True
        )
        
        context.add_output_metadata({
            "dbt_output": MetadataValue.text(result.stdout),
            "models_run": MetadataValue.text("staging models")
        })
        
        return {"status": "success", "timestamp": datetime.now().isoformat()}
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DBT staging models failed: {e.stderr}")
        context.add_output_metadata({
            "error": MetadataValue.text(e.stderr),
            "status": MetadataValue.text("failed")
        })
        raise


@asset(
    description="Run DBT transformations to create intermediate models",
    deps=[dbt_staging_models],
    group_name="intermediate"
)
def dbt_intermediate_models(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run DBT intermediate models."""
    try:
        # Run DBT intermediate models
        result = subprocess.run(
            ["dbt", "run", "--select", "intermediate"],
            cwd="/dbt",
            capture_output=True,
            text=True,
            check=True
        )
        
        context.add_output_metadata({
            "dbt_output": MetadataValue.text(result.stdout),
            "models_run": MetadataValue.text("intermediate models")
        })
        
        return {"status": "success", "timestamp": datetime.now().isoformat()}
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DBT intermediate models failed: {e.stderr}")
        context.add_output_metadata({
            "error": MetadataValue.text(e.stderr),
            "status": MetadataValue.text("failed")
        })
        raise


@asset(
    description="Run DBT transformations to create mart models",
    deps=[dbt_intermediate_models],
    group_name="marts"
)
def dbt_mart_models(context: AssetExecutionContext) -> Dict[str, Any]:
    """Run DBT mart models."""
    try:
        # Run DBT mart models
        result = subprocess.run(
            ["dbt", "run", "--select", "marts"],
            cwd="/dbt",
            capture_output=True,
            text=True,
            check=True
        )
        
        context.add_output_metadata({
            "dbt_output": MetadataValue.text(result.stdout),
            "models_run": MetadataValue.text("mart models")
        })
        
        return {"status": "success", "timestamp": datetime.now().isoformat()}
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DBT mart models failed: {e.stderr}")
        context.add_output_metadata({
            "error": MetadataValue.text(e.stderr),
            "status": MetadataValue.text("failed")
        })
        raise


@asset(
    description="Final weather summary analytics table",
    deps=[dbt_mart_models],
    group_name="analytics"
)
def weather_analytics_summary(context: AssetExecutionContext) -> Dict[str, Any]:
    """Generate final weather analytics summary."""
    try:
        import duckdb
        
        # Connect to database and get summary stats
        with duckdb.connect("/data/weather.db") as conn:
            # Get record counts
            current_count = conn.execute("SELECT COUNT(*) FROM current_weather").fetchone()[0]
            hourly_count = conn.execute("SELECT COUNT(*) FROM hourly_weather").fetchone()[0]
            daily_count = conn.execute("SELECT COUNT(*) FROM daily_weather").fetchone()[0]
            
            # Get latest weather summary
            summary_count = conn.execute("SELECT COUNT(*) FROM weather_summary").fetchone()[0]
            trends_count = conn.execute("SELECT COUNT(*) FROM weather_trends").fetchone()[0]
            
            context.add_output_metadata({
                "raw_current_records": MetadataValue.int(current_count),
                "raw_hourly_records": MetadataValue.int(hourly_count),
                "raw_daily_records": MetadataValue.int(daily_count),
                "summary_records": MetadataValue.int(summary_count),
                "trends_records": MetadataValue.int(trends_count),
                "pipeline_status": MetadataValue.text("completed")
            })
        
        return {
            "status": "success", 
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "raw_records": current_count + hourly_count + daily_count,
                "analytics_records": summary_count + trends_count
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to generate analytics summary: {e}")
        context.add_output_metadata({
            "error": MetadataValue.text(str(e)),
            "status": MetadataValue.text("failed")
        })
        raise 