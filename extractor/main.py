#!/usr/bin/env python3
"""
Boston Weather Data Extractor

Fetches weather data from National Weather Service (NWS) API for Boston
and stores it in DuckDB for further processing.
"""

import time
import schedule
import requests
import structlog
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import os
import sys

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config, NWSConfig, DatabaseConfig, NWSAPIError, NWSGeographicError, NWSServiceUnavailableError
from nws_cache import NWSCache
from utils import (
    DatabaseManager, 
    format_weather_description,
    calculate_api_usage_stats,
    transform_nws_current_weather,
    transform_nws_hourly_forecast,
    transform_nws_daily_forecast,
    validate_nws_response
)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class WeatherExtractor:
    """Main weather data extractor class."""
    
    def __init__(self):
        self.lat = config.boston_lat
        self.lon = config.boston_lon
        self.db_path = config.duckdb_path
        
        # Initialize NWS cache with 1 hour TTL
        self.nws_cache = NWSCache(cache_ttl=3600)
        
        # Validate coordinates are within NWS coverage
        if not NWSConfig.validate_coordinates(self.lat, self.lon):
            raise NWSGeographicError(f"Coordinates ({self.lat}, {self.lon}) are outside NWS coverage area")
        
        # Initialize database
        self._initialize_database()
        
        # Database manager reference
        self.db_manager = DatabaseManager
    
    def _initialize_database(self):
        """Initialize the database with required tables."""
        try:
            # Create data directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            with DatabaseManager(self.db_path) as db:
                db.initialize_database()
            logger.info("Database initialization completed")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _make_nws_request(self, url: str) -> Optional[Dict[str, Any]]:
        """Make HTTP request to NWS API with proper headers and retry logic."""
        headers = NWSConfig.get_headers()
        
        for attempt in range(NWSConfig.RETRY_ATTEMPTS):
            try:
                logger.info(f"Making NWS API request to: {url} (attempt {attempt + 1}/{NWSConfig.RETRY_ATTEMPTS})")
                
                response = requests.get(url, headers=headers, timeout=NWSConfig.TIMEOUT)
                
                # Handle NWS-specific error responses
                if response.status_code == 404:
                    raise NWSGeographicError("Location outside NWS coverage area")
                elif response.status_code == 503:
                    # Service unavailable - retry with exponential backoff
                    if attempt < NWSConfig.RETRY_ATTEMPTS - 1:
                        wait_time = NWSConfig.RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"NWS API temporarily unavailable, retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise NWSServiceUnavailableError("NWS API temporarily unavailable after all retries")
                elif response.status_code != 200:
                    logger.error(f"NWS API request failed with status {response.status_code}: {response.text}")
                    return None
                
                data = response.json()
                logger.debug("NWS API request successful")
                return data
                
            except NWSGeographicError:
                raise  # Don't retry geographic errors
            except NWSServiceUnavailableError:
                raise  # Don't retry if we've exhausted attempts
            except requests.exceptions.RequestException as e:
                if attempt < NWSConfig.RETRY_ATTEMPTS - 1:
                    wait_time = NWSConfig.RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"NWS API request failed: {e}, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"NWS API request failed after all retries: {e}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error during NWS API request: {e}")
                return None
        
        return None
    
    def _get_nws_metadata(self) -> Optional[Dict[str, Any]]:
        """Get NWS point metadata including forecast URLs. Uses caching to avoid redundant API calls."""
        try:
            # Check cache first
            cached_data = self.nws_cache.get_cached_points(self.lat, self.lon)
            if cached_data is not None:
                logger.debug(f"Using cached NWS metadata for coordinates ({self.lat}, {self.lon})")
                return cached_data
            
            # Fetch from API if not cached
            points_url = NWSConfig.get_points_url(self.lat, self.lon)
            points_data = self._make_nws_request(points_url)
            
            if not points_data or not validate_nws_response(points_data, 'points'):
                logger.error("Failed to get valid NWS points metadata")
                return None
            
            # Cache the result
            self.nws_cache.cache_points_data(self.lat, self.lon, points_data)
            
            logger.info("Successfully retrieved and cached NWS points metadata")
            return points_data
            
        except Exception as e:
            logger.error(f"Failed to get NWS metadata: {e}")
            return None
    
    def _fetch_current_conditions(self, station_url: str) -> Optional[Dict[str, Any]]:
        """Fetch current weather from nearest station."""
        try:
            # Get list of stations first
            stations_data = self._make_nws_request(station_url)
            if not stations_data or 'features' not in stations_data:
                logger.error("Failed to get NWS stations data")
                return None
            
            # Get the first available station
            stations = stations_data.get('features', [])
            if not stations:
                logger.error("No weather stations found")
                return None
            
            # Try to get current conditions from the first station
            station_id = stations[0].get('properties', {}).get('stationIdentifier')
            if not station_id:
                logger.error("No station identifier found")
                return None
            
            current_url = f"{NWSConfig.BASE_URL}/stations/{station_id}/observations/latest"
            current_data = self._make_nws_request(current_url)
            
            if not current_data or not validate_nws_response(current_data, 'current'):
                logger.error("Failed to get valid current conditions")
                return None
            
            logger.info("Successfully retrieved current conditions")
            return current_data
            
        except Exception as e:
            logger.error(f"Failed to fetch current conditions: {e}")
            return None
    
    def _fetch_hourly_forecast(self, forecast_url: str) -> Optional[Dict[str, Any]]:
        """Fetch hourly forecast data."""
        try:
            hourly_data = self._make_nws_request(forecast_url)
            
            if not hourly_data or not validate_nws_response(hourly_data, 'hourly'):
                logger.error("Failed to get valid hourly forecast")
                return None
            
            logger.info("Successfully retrieved hourly forecast")
            return hourly_data
            
        except Exception as e:
            logger.error(f"Failed to fetch hourly forecast: {e}")
            return None
    
    def _fetch_daily_forecast(self, forecast_url: str) -> Optional[Dict[str, Any]]:
        """Fetch daily forecast data."""
        try:
            daily_data = self._make_nws_request(forecast_url)
            
            if not daily_data or not validate_nws_response(daily_data, 'daily'):
                logger.error("Failed to get valid daily forecast")
                return None
            
            logger.info("Successfully retrieved daily forecast")
            return daily_data
            
        except Exception as e:
            logger.error(f"Failed to fetch daily forecast: {e}")
            return None
    
    def extract_and_store_weather_data(self):
        """Extract weather data from NWS API and store in database."""
        try:
            # Step 1: Get NWS metadata (points API)
            points_data = self._get_nws_metadata()
            if not points_data:
                logger.error("Failed to get NWS metadata")
                return
            
            properties = points_data.get('properties', {})
            forecast_url = properties.get('forecast')
            forecast_hourly_url = properties.get('forecastHourly')
            stations_url = properties.get('observationStations')
            
            if not all([forecast_url, forecast_hourly_url, stations_url]):
                logger.error("Missing required URLs in NWS points response")
                return
            
            # Step 2: Fetch all weather data
            current_data = self._fetch_current_conditions(stations_url)
            hourly_data = self._fetch_hourly_forecast(forecast_hourly_url)
            daily_data = self._fetch_daily_forecast(forecast_url)
            
            # Step 3: Transform data to match existing schema
            transformed_current = None
            transformed_hourly = []
            transformed_daily = []
            
            if current_data:
                transformed_current = transform_nws_current_weather(current_data)
            
            if hourly_data:
                transformed_hourly = transform_nws_hourly_forecast(hourly_data)
            
            if daily_data:
                transformed_daily = transform_nws_daily_forecast(daily_data)
            
            # Step 4: Store data in database
            with DatabaseManager(self.db_path) as db:
                # Store current weather
                if transformed_current:
                    db.insert_current_weather(transformed_current)
                    
                    # Log current weather info
                    description = format_weather_description(transformed_current)
                    logger.info(f"Current weather in Boston: {description}")
                
                # Store hourly weather
                if transformed_hourly:
                    db.insert_hourly_weather(transformed_hourly)
                
                # Store daily weather
                if transformed_daily:
                    db.insert_daily_weather(transformed_daily)
            
            logger.info("NWS weather data extraction and storage completed successfully")
            
        except NWSGeographicError as e:
            logger.error(f"Geographic error: {e}")
        except NWSServiceUnavailableError as e:
            logger.error(f"NWS service unavailable: {e}")
        except NWSAPIError as e:
            logger.error(f"NWS API error: {e}")
        except Exception as e:
            logger.error(f"Failed to extract and store weather data: {e}")
    
    def extract_current_and_hourly(self):
        """Extract current and hourly weather data (called every 10 minutes)."""
        logger.info("Starting current and hourly weather extraction")
        self.extract_and_store_weather_data()
    
    def extract_daily(self):
        """Extract daily weather data (called once per day)."""
        logger.info("Starting daily weather extraction")
        self.extract_and_store_weather_data()
    
    def log_stats(self):
        """Log current statistics."""
        try:
            stats = calculate_api_usage_stats()
            if stats:
                logger.info("Current pipeline statistics", **stats)
            
            # Log cache statistics
            cache_stats = self.nws_cache.get_cache_stats()
            logger.info("NWS cache statistics", **cache_stats)
            
        except Exception as e:
            logger.error(f"Failed to log statistics: {e}")
    
    def cleanup_cache(self):
        """Clean up expired cache entries."""
        try:
            removed_count = self.nws_cache.cleanup_expired()
            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} expired cache entries")
        except Exception as e:
            logger.error(f"Failed to cleanup cache: {e}")
    
    def run_scheduler(self):
        """Run the scheduled weather data extraction."""
        logger.info("Starting weather data extraction scheduler")
        
        # Schedule current and hourly weather extraction (every 10 minutes)
        schedule.every(10).minutes.do(self.extract_current_and_hourly)
        
        # Schedule daily weather extraction (once per day at 6 AM)
        schedule.every().day.at("06:00").do(self.extract_daily)
        
        # Schedule statistics logging (every hour)
        schedule.every().hour.do(self.log_stats)
        
        # Schedule cache cleanup (every 2 hours)
        schedule.every(2).hours.do(self.cleanup_cache)
        
        # Run initial extraction
        logger.info("Running initial weather data extraction")
        self.extract_and_store_weather_data()
        
        # Keep the scheduler running
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(60)  # Wait before retrying


def main():
    """Main entry point."""
    try:
        logger.info("Starting Boston Weather Data Extractor")
        logger.info(f"Configuration: lat={config.boston_lat}, lon={config.boston_lon}")
        
        # Create extractor instance
        extractor = WeatherExtractor()
        
        # Run the scheduler
        extractor.run_scheduler()
        
    except KeyboardInterrupt:
        logger.info("Weather extractor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in weather extractor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 