#!/usr/bin/env python3
"""
Simplified end-to-end testing for NWS API migration.

This script performs focused testing of the complete data extraction workflow
and validates that the NWS integration works correctly.
"""

import os
import sys
import time
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import logging

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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleEndToEndTest:
    """Simplified end-to-end test for NWS API integration."""
    
    def __init__(self):
        self.temp_db_path = None
        self.original_db_path = config.duckdb_path
        
    def setup_test_database(self):
        """Set up temporary test database."""
        temp_dir = tempfile.mkdtemp()
        self.temp_db_path = os.path.join(temp_dir, 'test_weather.db')
        
        # Initialize test database
        with DatabaseManager(self.temp_db_path) as db:
            db.initialize_database()
        
        logger.info(f"Test database created: {self.temp_db_path}")
    
    def cleanup_test_database(self):
        """Clean up test database."""
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            logger.info("Test database cleaned up")
    
    def test_nws_api_connectivity(self) -> bool:
        """Test basic NWS API connectivity."""
        logger.info("Testing NWS API connectivity...")
        
        try:
            import requests
            
            # Test points API
            points_url = NWSConfig.get_points_url(config.boston_lat, config.boston_lon)
            headers = NWSConfig.get_headers()
            
            response = requests.get(points_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if validate_nws_response(data, 'points'):
                    logger.info("✓ NWS API connectivity test passed")
                    return True
                else:
                    logger.error("✗ NWS API response validation failed")
                    return False
            else:
                logger.error(f"✗ NWS API request failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"✗ NWS API connectivity test failed: {e}")
            return False
    
    def test_data_transformation(self) -> bool:
        """Test NWS data transformation functions."""
        logger.info("Testing data transformation functions...")
        
        try:
            # Mock NWS current weather response
            mock_current = {
                "properties": {
                    "timestamp": "2024-01-15T10:00:00+00:00",
                    "temperature": {"value": 5.0, "unitCode": "wmoUnit:degC"},
                    "relativeHumidity": {"value": 65},
                    "windSpeed": {"value": 3.5, "unitCode": "wmoUnit:m_s-1"},
                    "windDirection": {"value": 180},
                    "barometricPressure": {"value": 101325, "unitCode": "wmoUnit:Pa"},
                    "textDescription": "Partly Cloudy"
                }
            }
            
            # Test current weather transformation
            transformed_current = transform_nws_current_weather(mock_current)
            if not transformed_current:
                logger.error("✗ Current weather transformation failed")
                return False
            
            # Validate transformed data structure
            required_fields = ['timestamp', 'temp', 'feels_like', 'humidity', 'pressure', 
                             'wind_speed', 'wind_deg', 'description', 'icon']
            
            for field in required_fields:
                if field not in transformed_current:
                    logger.error(f"✗ Missing field in transformed data: {field}")
                    return False
            
            logger.info("✓ Data transformation test passed")
            return True
            
        except Exception as e:
            logger.error(f"✗ Data transformation test failed: {e}")
            return False
    
    def test_database_operations(self) -> bool:
        """Test database operations with transformed data."""
        logger.info("Testing database operations...")
        
        try:
            # Create sample data
            current_data = {
                'timestamp': datetime.now(timezone.utc),
                'temp': 15.5,
                'feels_like': 14.2,
                'humidity': 65,
                'pressure': 1013,
                'wind_speed': 3.5,
                'wind_deg': 180,
                'description': 'Partly Cloudy',
                'icon': '02d'
            }
            
            hourly_data = [{
                'timestamp': datetime.now(timezone.utc),
                'temp': 16.0,
                'feels_like': 15.0,
                'humidity': 60,
                'pressure': 1014,
                'wind_speed': 4.0,
                'wind_deg': 190,
                'description': 'Clear',
                'icon': '01d',
                'pop': 0.1
            }]
            
            daily_data = [{
                'date': datetime.now(timezone.utc).date(),
                'temp_min': 10.0,
                'temp_max': 20.0,
                'temp_day': 18.0,
                'temp_night': 12.0,
                'humidity': 55,
                'pressure': 1015,
                'wind_speed': 3.0,
                'wind_deg': 200,
                'description': 'Sunny',
                'icon': '01d',
                'pop': 0.0
            }]
            
            # Test database operations
            with DatabaseManager(self.temp_db_path) as db:
                # Insert data
                db.insert_current_weather(current_data)
                db.insert_hourly_weather(hourly_data)
                db.insert_daily_weather(daily_data)
                
                # Verify data was inserted
                current_count = db.execute_query("SELECT COUNT(*) as count FROM current_weather")
                hourly_count = db.execute_query("SELECT COUNT(*) as count FROM hourly_weather")
                daily_count = db.execute_query("SELECT COUNT(*) as count FROM daily_weather")
                
                if (current_count[0]['count'] == 1 and 
                    hourly_count[0]['count'] == 1 and 
                    daily_count[0]['count'] == 1):
                    logger.info("✓ Database operations test passed")
                    return True
                else:
                    logger.error("✗ Database operations test failed - incorrect record counts")
                    return False
                    
        except Exception as e:
            logger.error(f"✗ Database operations test failed: {e}")
            return False
    
    def test_complete_workflow(self) -> bool:
        """Test the complete data extraction workflow."""
        logger.info("Testing complete workflow...")
        
        try:
            # Import here to avoid circular imports
            from extractor.main import WeatherExtractor
            
            # Create extractor with test database
            original_path = config.duckdb_path
            config.duckdb_path = self.temp_db_path
            
            try:
                extractor = WeatherExtractor()
                
                # Run extraction
                extractor.extract_and_store_weather_data()
                
                # Verify data was stored
                with DatabaseManager(self.temp_db_path) as db:
                    current_count = db.execute_query("SELECT COUNT(*) as count FROM current_weather")
                    hourly_count = db.execute_query("SELECT COUNT(*) as count FROM hourly_weather")
                    daily_count = db.execute_query("SELECT COUNT(*) as count FROM daily_weather")
                    
                    if (current_count and current_count[0]['count'] > 0 and
                        hourly_count and hourly_count[0]['count'] > 0 and
                        daily_count and daily_count[0]['count'] > 0):
                        logger.info(f"✓ Complete workflow test passed - "
                                  f"Current: {current_count[0]['count']}, "
                                  f"Hourly: {hourly_count[0]['count']}, "
                                  f"Daily: {daily_count[0]['count']}")
                        return True
                    else:
                        logger.error("✗ Complete workflow test failed - no data extracted")
                        return False
                        
            finally:
                config.duckdb_path = original_path
                
        except Exception as e:
            logger.error(f"✗ Complete workflow test failed: {e}")
            return False
    
    def test_data_quality(self) -> bool:
        """Test data quality and schema compliance."""
        logger.info("Testing data quality...")
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Test current weather data quality
                current_data = db.execute_query("""
                    SELECT 
                        COUNT(*) as total,
                        AVG(temp) as avg_temp,
                        MIN(temp) as min_temp,
                        MAX(temp) as max_temp,
                        AVG(humidity) as avg_humidity,
                        AVG(pressure) as avg_pressure
                    FROM current_weather
                """)
                
                if current_data and len(current_data) > 0:
                    data = current_data[0]
                    
                    # Basic sanity checks for Boston weather
                    if not (-30 <= data['avg_temp'] <= 40):
                        logger.warning(f"Temperature seems unusual: {data['avg_temp']}°C")
                    
                    if not (0 <= data['avg_humidity'] <= 100):
                        logger.error(f"Invalid humidity: {data['avg_humidity']}%")
                        return False
                    
                    if not (900 <= data['avg_pressure'] <= 1100):
                        logger.error(f"Invalid pressure: {data['avg_pressure']} hPa")
                        return False
                    
                    logger.info("✓ Data quality test passed")
                    return True
                else:
                    logger.error("✗ No data found for quality testing")
                    return False
                    
        except Exception as e:
            logger.error(f"✗ Data quality test failed: {e}")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all tests and return results."""
        logger.info("Starting NWS API end-to-end validation...")
        
        results = {}
        
        try:
            self.setup_test_database()
            
            # Run tests
            results['api_connectivity'] = self.test_nws_api_connectivity()
            results['data_transformation'] = self.test_data_transformation()
            results['database_operations'] = self.test_database_operations()
            results['complete_workflow'] = self.test_complete_workflow()
            results['data_quality'] = self.test_data_quality()
            
            return results
            
        finally:
            self.cleanup_test_database()


def main():
    """Main entry point."""
    print("=" * 60)
    print("NWS API Migration - End-to-End Validation")
    print("=" * 60)
    
    tester = SimpleEndToEndTest()
    
    try:
        results = tester.run_all_tests()
        
        # Print results
        print("\nTest Results:")
        print("-" * 40)
        
        all_passed = True
        for test_name, result in results.items():
            status = "PASSED" if result else "FAILED"
            color = "\033[92m" if result else "\033[91m"
            print(f"{color}{test_name:20} {status}\033[0m")
            if not result:
                all_passed = False
        
        # Overall result
        print("\n" + "=" * 60)
        if all_passed:
            print("\033[92m✓ ALL TESTS PASSED - NWS API migration is successful!\033[0m")
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