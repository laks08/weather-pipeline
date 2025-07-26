#!/usr/bin/env python3
"""
Core functionality test for NWS API integration.

This script tests the essential components of the NWS API migration
without relying on the full WeatherExtractor class.
"""

import os
import sys
import tempfile
from datetime import datetime, timezone
import logging
import requests

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from extractor.config import config, NWSConfig
from extractor.utils import (
    DatabaseManager,
    transform_nws_current_weather,
    transform_nws_hourly_forecast,
    transform_nws_daily_forecast,
    validate_nws_response
)
from extractor.nws_cache import NWSCache

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NWSCoreFunctionalityTest:
    """Test core NWS API functionality."""
    
    def __init__(self):
        self.temp_db_path = None
        
    def setup_test_database(self):
        """Set up temporary test database."""
        temp_dir = tempfile.mkdtemp()
        self.temp_db_path = os.path.join(temp_dir, 'test_weather.db')
        
        with DatabaseManager(self.temp_db_path) as db:
            db.initialize_database()
        
        logger.info(f"Test database created: {self.temp_db_path}")
    
    def cleanup_test_database(self):
        """Clean up test database."""
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            logger.info("Test database cleaned up")
    
    def test_nws_api_real_data_extraction(self) -> bool:
        """Test real NWS API data extraction and transformation."""
        logger.info("Testing real NWS API data extraction...")
        
        try:
            # Step 1: Get NWS metadata
            points_url = NWSConfig.get_points_url(config.boston_lat, config.boston_lon)
            headers = NWSConfig.get_headers()
            
            response = requests.get(points_url, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to get NWS points data: {response.status_code}")
                return False
            
            points_data = response.json()
            if not validate_nws_response(points_data, 'points'):
                logger.error("Invalid NWS points response")
                return False
            
            properties = points_data.get('properties', {})
            forecast_url = properties.get('forecast')
            forecast_hourly_url = properties.get('forecastHourly')
            stations_url = properties.get('observationStations')
            
            if not all([forecast_url, forecast_hourly_url, stations_url]):
                logger.error("Missing required URLs in NWS points response")
                return False
            
            logger.info("✓ NWS points API working")
            
            # Step 2: Get current conditions
            current_data = None
            try:
                stations_response = requests.get(stations_url, headers=headers, timeout=30)
                if stations_response.status_code == 200:
                    stations_data = stations_response.json()
                    stations = stations_data.get('features', [])
                    
                    if stations:
                        station_id = stations[0].get('properties', {}).get('stationIdentifier')
                        if station_id:
                            current_url = f"{NWSConfig.BASE_URL}/stations/{station_id}/observations/latest"
                            current_response = requests.get(current_url, headers=headers, timeout=30)
                            if current_response.status_code == 200:
                                current_data = current_response.json()
                                logger.info("✓ NWS current conditions API working")
            except Exception as e:
                logger.warning(f"Current conditions test failed: {e}")
            
            # Step 3: Get hourly forecast
            hourly_data = None
            try:
                hourly_response = requests.get(forecast_hourly_url, headers=headers, timeout=30)
                if hourly_response.status_code == 200:
                    hourly_data = hourly_response.json()
                    if validate_nws_response(hourly_data, 'hourly'):
                        logger.info("✓ NWS hourly forecast API working")
            except Exception as e:
                logger.warning(f"Hourly forecast test failed: {e}")
            
            # Step 4: Get daily forecast
            daily_data = None
            try:
                daily_response = requests.get(forecast_url, headers=headers, timeout=30)
                if daily_response.status_code == 200:
                    daily_data = daily_response.json()
                    if validate_nws_response(daily_data, 'daily'):
                        logger.info("✓ NWS daily forecast API working")
            except Exception as e:
                logger.warning(f"Daily forecast test failed: {e}")
            
            # Step 5: Transform and store data
            records_stored = 0
            
            with DatabaseManager(self.temp_db_path) as db:
                # Transform and store current weather
                if current_data:
                    transformed_current = transform_nws_current_weather(current_data)
                    if transformed_current:
                        db.insert_current_weather(transformed_current)
                        records_stored += 1
                        logger.info("✓ Current weather transformed and stored")
                
                # Transform and store hourly forecast
                if hourly_data:
                    transformed_hourly = transform_nws_hourly_forecast(hourly_data)
                    if transformed_hourly:
                        db.insert_hourly_weather(transformed_hourly)
                        records_stored += len(transformed_hourly)
                        logger.info(f"✓ {len(transformed_hourly)} hourly records transformed and stored")
                
                # Transform and store daily forecast
                if daily_data:
                    transformed_daily = transform_nws_daily_forecast(daily_data)
                    if transformed_daily:
                        db.insert_daily_weather(transformed_daily)
                        records_stored += len(transformed_daily)
                        logger.info(f"✓ {len(transformed_daily)} daily records transformed and stored")
            
            if records_stored > 0:
                logger.info(f"✓ Real NWS API data extraction successful - {records_stored} total records")
                return True
            else:
                logger.error("✗ No data was successfully extracted and stored")
                return False
                
        except Exception as e:
            logger.error(f"✗ Real NWS API data extraction failed: {e}")
            return False
    
    def test_data_schema_compliance(self) -> bool:
        """Test that stored data complies with expected schema."""
        logger.info("Testing data schema compliance...")
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Check current weather schema
                current_schema = db.execute_query("PRAGMA table_info(current_weather)")
                expected_current_columns = {
                    'timestamp', 'temp', 'feels_like', 'humidity', 'pressure',
                    'wind_speed', 'wind_deg', 'description', 'icon'
                }
                actual_current_columns = {row['name'] for row in current_schema}
                
                if not expected_current_columns.issubset(actual_current_columns):
                    missing = expected_current_columns - actual_current_columns
                    logger.error(f"Missing columns in current_weather: {missing}")
                    return False
                
                # Check hourly weather schema
                hourly_schema = db.execute_query("PRAGMA table_info(hourly_weather)")
                expected_hourly_columns = {
                    'timestamp', 'temp', 'feels_like', 'humidity', 'pressure',
                    'wind_speed', 'wind_deg', 'description', 'icon', 'pop'
                }
                actual_hourly_columns = {row['name'] for row in hourly_schema}
                
                if not expected_hourly_columns.issubset(actual_hourly_columns):
                    missing = expected_hourly_columns - actual_hourly_columns
                    logger.error(f"Missing columns in hourly_weather: {missing}")
                    return False
                
                # Check daily weather schema
                daily_schema = db.execute_query("PRAGMA table_info(daily_weather)")
                expected_daily_columns = {
                    'date', 'temp_min', 'temp_max', 'temp_day', 'temp_night',
                    'humidity', 'pressure', 'wind_speed', 'wind_deg',
                    'description', 'icon', 'pop'
                }
                actual_daily_columns = {row['name'] for row in daily_schema}
                
                if not expected_daily_columns.issubset(actual_daily_columns):
                    missing = expected_daily_columns - actual_daily_columns
                    logger.error(f"Missing columns in daily_weather: {missing}")
                    return False
                
                logger.info("✓ Data schema compliance test passed")
                return True
                
        except Exception as e:
            logger.error(f"✗ Data schema compliance test failed: {e}")
            return False
    
    def test_data_quality_validation(self) -> bool:
        """Test data quality and reasonable values."""
        logger.info("Testing data quality validation...")
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Test current weather data quality
                current_data = db.execute_query("""
                    SELECT 
                        COUNT(*) as total,
                        AVG(temp) as avg_temp,
                        AVG(humidity) as avg_humidity,
                        AVG(pressure) as avg_pressure,
                        COUNT(CASE WHEN temp IS NOT NULL THEN 1 END) as temp_count,
                        COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as desc_count
                    FROM current_weather
                """)
                
                if current_data and current_data[0]['total'] > 0:
                    data = current_data[0]
                    
                    # Check data completeness
                    if data['temp_count'] == 0:
                        logger.error("No temperature data found")
                        return False
                    
                    if data['desc_count'] == 0:
                        logger.error("No weather descriptions found")
                        return False
                    
                    # Check reasonable values for Boston
                    if data['avg_temp'] is not None and not (-40 <= data['avg_temp'] <= 45):
                        logger.warning(f"Temperature seems extreme: {data['avg_temp']}°C")
                    
                    if data['avg_humidity'] is not None and not (0 <= data['avg_humidity'] <= 100):
                        logger.error(f"Invalid humidity: {data['avg_humidity']}%")
                        return False
                    
                    logger.info("✓ Current weather data quality is good")
                
                # Test hourly weather data
                hourly_data = db.execute_query("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT DATE(timestamp)) as unique_days,
                        AVG(pop) as avg_pop
                    FROM hourly_weather
                """)
                
                if hourly_data and hourly_data[0]['total'] > 0:
                    data = hourly_data[0]
                    
                    if data['total'] < 12:  # Should have at least 12 hours
                        logger.warning(f"Limited hourly data: {data['total']} records")
                    
                    if data['avg_pop'] is not None and not (0 <= data['avg_pop'] <= 1):
                        logger.error(f"Invalid precipitation probability: {data['avg_pop']}")
                        return False
                    
                    logger.info(f"✓ Hourly weather data quality is good ({data['total']} records)")
                
                # Test daily weather data
                daily_data = db.execute_query("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN temp_max >= temp_min THEN 1 END) as logical_temp_count
                    FROM daily_weather
                    WHERE temp_max IS NOT NULL AND temp_min IS NOT NULL
                """)
                
                if daily_data and daily_data[0]['total'] > 0:
                    data = daily_data[0]
                    
                    if data['logical_temp_count'] != data['total']:
                        logger.error("Some daily records have max temp < min temp")
                        return False
                    
                    logger.info(f"✓ Daily weather data quality is good ({data['total']} records)")
                
                logger.info("✓ Data quality validation passed")
                return True
                
        except Exception as e:
            logger.error(f"✗ Data quality validation failed: {e}")
            return False
    
    def test_error_handling(self) -> bool:
        """Test error handling scenarios."""
        logger.info("Testing error handling...")
        
        try:
            # Test geographic validation
            if not NWSConfig.validate_coordinates(51.5074, -0.1278):  # London, UK
                logger.info("✓ Geographic validation correctly rejects non-US coordinates")
            else:
                logger.error("✗ Geographic validation failed")
                return False
            
            # Test invalid API response handling
            invalid_response = {"invalid": "data"}
            if not validate_nws_response(invalid_response, 'points'):
                logger.info("✓ Response validation correctly rejects invalid data")
            else:
                logger.error("✗ Response validation failed")
                return False
            
            # Test transformation with missing data
            incomplete_data = {"properties": {"temperature": {"value": 20}}}
            result = transform_nws_current_weather(incomplete_data)
            if result is not None:
                logger.info("✓ Transformation handles incomplete data gracefully")
            else:
                logger.warning("Transformation rejected incomplete data (may be expected)")
            
            logger.info("✓ Error handling tests passed")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error handling test failed: {e}")
            return False
    
    def run_all_tests(self) -> dict:
        """Run all core functionality tests."""
        logger.info("Starting NWS core functionality tests...")
        
        results = {}
        
        try:
            self.setup_test_database()
            
            # Run tests in order
            results['real_data_extraction'] = self.test_nws_api_real_data_extraction()
            results['schema_compliance'] = self.test_data_schema_compliance()
            results['data_quality'] = self.test_data_quality_validation()
            results['error_handling'] = self.test_error_handling()
            
            return results
            
        finally:
            self.cleanup_test_database()


def main():
    """Main entry point."""
    print("=" * 70)
    print("NWS API Migration - Core Functionality Test")
    print("=" * 70)
    
    tester = NWSCoreFunctionalityTest()
    
    try:
        results = tester.run_all_tests()
        
        # Print results
        print("\nTest Results:")
        print("-" * 50)
        
        all_passed = True
        for test_name, result in results.items():
            status = "PASSED" if result else "FAILED"
            color = "\033[92m" if result else "\033[91m"
            print(f"{color}{test_name:25} {status}\033[0m")
            if not result:
                all_passed = False
        
        # Overall result
        print("\n" + "=" * 70)
        if all_passed:
            print("\033[92m✓ ALL CORE FUNCTIONALITY TESTS PASSED!\033[0m")
            print("The NWS API integration is working correctly.")
            return 0
        else:
            print("\033[91m✗ SOME TESTS FAILED - Please review the results above\033[0m")
            return 1
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        print(f"\033[91m✗ TEST EXECUTION FAILED: {e}\033[0m")
        return 1


if __name__ == "__main__":
    sys.exit(main())