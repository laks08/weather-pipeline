"""
Main Dagster pipeline for the Boston Weather ETL process.
"""

from dagster import (
    Definitions, 
    define_asset_job, 
    ScheduleDefinition,
    DefaultScheduleStatus
)

from weather_pipeline.assets import (
    raw_weather_data,
    raw_current_weather,
    raw_hourly_weather, 
    raw_daily_weather,
    dbt_staging_models,
    dbt_intermediate_models,
    dbt_mart_models,
    weather_analytics_summary
)
from weather_pipeline.resources import duckdb_resource, weather_api_resource, dbt_resource

# All assets
all_assets = [
    raw_weather_data,
    raw_current_weather,
    raw_hourly_weather,
    raw_daily_weather,
    dbt_staging_models,
    dbt_intermediate_models,
    dbt_mart_models,
    weather_analytics_summary
]

# Define jobs
weather_etl_job = define_asset_job(
    name="weather_etl_job",
    selection=all_assets,
    description="Complete weather ETL pipeline"
)

current_weather_job = define_asset_job(
    name="current_weather_job",
    selection=[raw_weather_data, raw_current_weather, raw_hourly_weather],
    description="Current and hourly weather data extraction"
)

daily_weather_job = define_asset_job(
    name="daily_weather_job",
    selection=[raw_weather_data, raw_daily_weather],
    description="Daily weather forecast extraction"
)

# Define schedules
current_weather_schedule = ScheduleDefinition(
    job=current_weather_job,
    cron_schedule="*/10 * * * *",  # Every 10 minutes
    default_status=DefaultScheduleStatus.RUNNING,
    description="Extract current and hourly weather data every 10 minutes"
)

daily_weather_schedule = ScheduleDefinition(
    job=daily_weather_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Extract daily weather forecast once per day"
)

full_pipeline_schedule = ScheduleDefinition(
    job=weather_etl_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run complete ETL pipeline every 6 hours"
)

# Define the pipeline
defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_resource": duckdb_resource,
        "weather_api_resource": weather_api_resource,
        "dbt_resource": dbt_resource,
    },
    jobs=[
        weather_etl_job,
        current_weather_job,
        daily_weather_job,
    ],
    schedules=[
        current_weather_schedule,
        daily_weather_schedule,
        full_pipeline_schedule,
    ]
) 