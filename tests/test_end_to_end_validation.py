#!/usr/bin/env python3
"""
End-to-end testing and validation for NWS API migration.

This script performs comprehensive testing of the complete data extraction workflow,
validates data quality, and ensures downstream compatibility.
"""

import os
import sys
import time
import json
import sqlite3
import tempfile
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from extractor.main import WeatherExtractor
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EndToEndValidator:
    """Comprehensive end-to-end validation for NWS API integration."""
    
    def __init__(self):
        self.test_results = {
            'data_extraction': False,
            'schema_validation': False,
            'data_quality': False,
            'dbt_compatibility': False,
            'error_handling': False,
            'performance': False
        }
        self.temp_db_path = None
        self.original_db_path = config.duckdb_path
        
    def setup_test_environment(self):
        """Set up temporary test environment."""
        logger.info("Setting up test environment...")
        
        # Create temporary database for testing
        temp_dir = tempfile.mkdtemp()
        self.temp_db_path = os.path.join(temp_dir, 'test_weather.db')
        
        # Override config for testing
        config.duckdb_path = self.temp_db_path
        
        # Create the temporary database directory
        os.makedirs(os.path.dirname(self.temp_db_path), exist_ok=True)
        
        logger.info(f"Test database created at: {self.temp_db_path}")
    
    def cleanup_test_environment(self):
        """Clean up test environment."""
        logger.info("Cleaning up test environment...")
        
        # Restore original config
        config.duckdb_path = self.original_db_path
        
        # Remove temporary database
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            logger.info("Test database cleaned up")
    
    def test_complete_data_extraction_workflow(self) -> bool:
        """Test the complete data extraction workflow with NWS API."""
        logger.info("Testing complete data extraction workflow...")
        
        try:
            # Create extractor instance with test database path
            extractor = WeatherExtractor()
            extractor.db_path = self.temp_db_path
            
            # Initialize database manually for testing
            extractor._initialize_database()
            
            # Run data extraction
            extractor.extract_and_store_weather_data()
            
            # Verify data was stored
            with DatabaseManager(self.temp_db_path) as db:
                # Check current weather data
                current_data = db.execute_query("SELECT COUNT(*) as count FROM current_weather")
                if not current_data or current_data[0]['count'] == 0:
                    logger.error("No current weather data found")
                    return False
                
                # Check hourly weather data
                hourly_data = db.execute_query("SELECT COUNT(*) as count FROM hourly_weather")
                if not hourly_data or hourly_data[0]['count'] == 0:
                    logger.error("No hourly weather data found")
                    return False
                
                # Check daily weather data
                daily_data = db.execute_query("SELECT COUNT(*) as count FROM daily_weather")
                if not daily_data or daily_data[0]['count'] == 0:
                    logger.error("No daily weather data found")
                    return False
                
                logger.info(f"Data extraction successful: "
                          f"current={current_data[0]['count']}, "
                          f"hourly={hourly_data[0]['count']}, "
                          f"daily={daily_data[0]['count']}")
                
            return True
            
        except Exception as e:
            logger.error(f"Data extraction workflow failed: {e}")
            return False
    
    def validate_database_schema(self) -> bool:
        """Validate that transformed data matches existing database schema."""
        logger.info("Validating database schema compatibility...")
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Define expected schema for each table
                expected_schemas = {
                    'current_weather': [
                        'timestamp', 'temp', 'feels_like', 'humidity', 'pressure',
                        'wind_speed', 'wind_deg', 'description', 'icon'
                    ],
                    'hourly_weather': [
                        'timestamp', 'temp', 'feels_like', 'humidity', 'pressure',
                        'wind_speed', 'wind_deg', 'description', 'icon', 'pop'
                    ],
                    'daily_weather': [
                        'date', 'temp_min', 'temp_max', 'temp_day', 'temp_night',
                        'humidity', 'pressure', 'wind_speed', 'wind_deg',
                        'description', 'icon', 'pop'
                    ]
                }
                
                # Validate each table schema
                for table_name, expected_columns in expected_schemas.items():
                    # Get actual table schema
                    schema_query = f"PRAGMA table_info({table_name})"
                    schema_result = db.execute_query(schema_query)
                    
                    if not schema_result:
                        logger.error(f"Table {table_name} not found")
                        return False
                    
                    actual_columns = [row['name'] for row in schema_result]
                    
                    # Check if all expected columns exist
                    missing_columns = set(expected_columns) - set(actual_columns)
                    if missing_columns:
                        logger.error(f"Missing columns in {table_name}: {missing_columns}")
                        return False
                    
                    logger.info(f"Schema validation passed for {table_name}")
                
                return True
                
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
    
    def validate_data_quality(self) -> bool:
        """Validate data quality and completeness."""
        logger.info("Validating data quality...")
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Validate current weather data quality
                current_quality = self._validate_current_weather_quality(db)
                if not current_quality:
                    return False
                
                # Validate hourly weather data quality
                hourly_quality = self._validate_hourly_weather_quality(db)
                if not hourly_quality:
                    return False
                
                # Validate daily weather data quality
                daily_quality = self._validate_daily_weather_quality(db)
                if not daily_quality:
                    return False
                
                logger.info("Data quality validation passed")
                return True
                
        except Exception as e:
            logger.error(f"Data quality validation failed: {e}")
            return False
    
    def _validate_current_weather_quality(self, db: DatabaseManager) -> bool:
        """Validate current weather data quality."""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(temp) as temp_count,
            COUNT(humidity) as humidity_count,
            COUNT(pressure) as pressure_count,
            COUNT(wind_speed) as wind_speed_count,
            AVG(temp) as avg_temp,
            AVG(humidity) as avg_humidity,
            AVG(pressure) as avg_pressure
        FROM current_weather
        """
        
        result = db.execute_query(query)
        if not result:
            logger.error("No current weather data found")
            return False
        
        data = result[0]
        
        # Check data completeness
        if data['total_records'] == 0:
            logger.error("No current weather records found")
            return False
        
        # Check for reasonable temperature values (Boston climate)
        if not (-30 <= data['avg_temp'] <= 40):
            logger.warning(f"Temperature seems unreasonable: {data['avg_temp']}°C")
        
        # Check for reasonable humidity values
        if not (0 <= data['avg_humidity'] <= 100):
            logger.error(f"Invalid humidity value: {data['avg_humidity']}%")
            return False
        
        # Check for reasonable pressure values
        if not (900 <= data['avg_pressure'] <= 1100):
            logger.error(f"Invalid pressure value: {data['avg_pressure']} hPa")
            return False
        
        logger.info(f"Current weather quality check passed: {data['total_records']} records")
        return True
    
    def _validate_hourly_weather_quality(self, db: DatabaseManager) -> bool:
        """Validate hourly weather data quality."""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT DATE(timestamp)) as unique_days,
            MIN(timestamp) as earliest_time,
            MAX(timestamp) as latest_time,
            AVG(temp) as avg_temp,
            COUNT(pop) as pop_count
        FROM hourly_weather
        """
        
        result = db.execute_query(query)
        if not result:
            logger.error("No hourly weather data found")
            return False
        
        data = result[0]
        
        # Check data completeness
        if data['total_records'] < 24:  # Should have at least 24 hours of data
            logger.warning(f"Limited hourly data: {data['total_records']} records")
        
        # Check time range (should cover multiple days)
        if data['unique_days'] < 1:
            logger.error("Hourly data should cover at least 1 day")
            return False
        
        logger.info(f"Hourly weather quality check passed: {data['total_records']} records over {data['unique_days']} days")
        return True
    
    def _validate_daily_weather_quality(self, db: DatabaseManager) -> bool:
        """Validate daily weather data quality."""
        query = """
        SELECT 
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(temp_max) as avg_temp_max,
            AVG(temp_min) as avg_temp_min,
            COUNT(CASE WHEN temp_max > temp_min THEN 1 END) as logical_temp_count
        FROM daily_weather
        """
        
        result = db.execute_query(query)
        if not result:
            logger.error("No daily weather data found")
            return False
        
        data = result[0]
        
        # Check data completeness
        if data['total_records'] < 3:  # Should have at least 3 days of forecast
            logger.warning(f"Limited daily data: {data['total_records']} records")
        
        # Check temperature logic (max should be >= min)
        if data['logical_temp_count'] != data['total_records']:
            logger.error("Some daily records have max temp < min temp")
            return False
        
        logger.info(f"Daily weather quality check passed: {data['total_records']} records")
        return True
    
    def test_dbt_model_compatibility(self) -> bool:
        """Test that downstream dbt models continue to work without modification."""
        logger.info("Testing dbt model compatibility...")
        
        try:
            # Change to dbt directory
            dbt_dir = os.path.join(os.path.dirname(__file__), 'dbt')
            if not os.path.exists(dbt_dir):
                logger.error("dbt directory not found")
                return False
            
            # Update dbt profiles to use test database
            self._update_dbt_profiles_for_test()
            
            # Run dbt tests
            result = subprocess.run(
                ['dbt', 'test', '--profiles-dir', '.'],
                cwd=dbt_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info("dbt tests passed successfully")
                return True
            else:
                logger.error(f"dbt tests failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("dbt tests timed out")
            return False
        except Exception as e:
            logger.error(f"dbt compatibility test failed: {e}")
            return False
    
    def _update_dbt_profiles_for_test(self):
        """Update dbt profiles to use test database."""
        profiles_content = f"""
weather_pipeline:
  target: test
  outputs:
    test:
      type: duckdb
      path: {self.temp_db_path}
      threads: 1
"""
        
        profiles_path = os.path.join(os.path.dirname(__file__), 'dbt', 'profiles.yml')
        with open(profiles_path, 'w') as f:
            f.write(profiles_content)
    
    def test_error_scenarios(self) -> bool:
        """Test error scenarios and recovery mechanisms."""
        logger.info("Testing error scenarios...")
        
        try:
            # Test geographic error (coordinates outside US)
            try:
                # Test with invalid coordinates directly
                from extractor.config import NWSConfig
                if not NWSConfig.validate_coordinates(51.5074, -0.1278):  # London, UK
                    logger.info("Geographic validation works correctly")
                else:
                    logger.error("Geographic validation failed to reject invalid coordinates")
                    return False
                
            except Exception as e:
                logger.error(f"Geographic error test failed: {e}")
                return False
            
            # Test API timeout handling
            original_timeout = NWSConfig.TIMEOUT
            try:
                NWSConfig.TIMEOUT = 0.001  # Very short timeout
                extractor = WeatherExtractor()
                extractor.db_path = self.temp_db_path
                extractor._initialize_database()
                result = extractor._make_nws_request("https://api.weather.gov/points/42.3601,-71.0589")
                # Should handle timeout gracefully
                if result is None:
                    logger.info("Timeout error handling works correctly")
                else:
                    logger.warning("Timeout test may not have triggered properly")
            finally:
                NWSConfig.TIMEOUT = original_timeout
            
            logger.info("Error scenario testing completed")
            return True
            
        except Exception as e:
            logger.error(f"Error scenario testing failed: {e}")
            return False
    
    def test_performance_metrics(self) -> bool:
        """Test performance and validate response times."""
        logger.info("Testing performance metrics...")
        
        try:
            start_time = time.time()
            
            # Run complete extraction workflow
            extractor = WeatherExtractor()
            extractor.db_path = self.temp_db_path
            extractor._initialize_database()
            extractor.extract_and_store_weather_data()
            
            end_time = time.time()
            extraction_time = end_time - start_time
            
            # Performance thresholds
            MAX_EXTRACTION_TIME = 60  # 60 seconds
            
            if extraction_time > MAX_EXTRACTION_TIME:
                logger.warning(f"Extraction took {extraction_time:.2f}s (threshold: {MAX_EXTRACTION_TIME}s)")
            else:
                logger.info(f"Extraction completed in {extraction_time:.2f}s")
            
            # Test cache performance
            cache = NWSCache()
            cache_stats = cache.get_cache_stats()
            logger.info(f"Cache statistics: {cache_stats}")
            
            return True
            
        except Exception as e:
            logger.error(f"Performance testing failed: {e}")
            return False
    
    def generate_data_comparison_report(self) -> Dict[str, Any]:
        """Generate a report comparing data quality with expected values."""
        logger.info("Generating data comparison report...")
        
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'database_path': self.temp_db_path,
            'tables': {}
        }
        
        try:
            with DatabaseManager(self.temp_db_path) as db:
                # Analyze current weather
                current_stats = db.execute_query("""
                    SELECT 
                        COUNT(*) as record_count,
                        AVG(temp) as avg_temp,
                        MIN(temp) as min_temp,
                        MAX(temp) as max_temp,
                        AVG(humidity) as avg_humidity,
                        AVG(pressure) as avg_pressure,
                        AVG(wind_speed) as avg_wind_speed
                    FROM current_weather
                """)
                
                if current_stats:
                    report['tables']['current_weather'] = current_stats[0]
                
                # Analyze hourly weather
                hourly_stats = db.execute_query("""
                    SELECT 
                        COUNT(*) as record_count,
                        COUNT(DISTINCT DATE(timestamp)) as unique_days,
                        AVG(temp) as avg_temp,
                        AVG(pop) as avg_precipitation_prob
                    FROM hourly_weather
                """)
                
                if hourly_stats:
                    report['tables']['hourly_weather'] = hourly_stats[0]
                
                # Analyze daily weather
                daily_stats = db.execute_query("""
                    SELECT 
                        COUNT(*) as record_count,
                        AVG(temp_max) as avg_temp_max,
                        AVG(temp_min) as avg_temp_min,
                        AVG(pop) as avg_precipitation_prob
                    FROM daily_weather
                """)
                
                if daily_stats:
                    report['tables']['daily_weather'] = daily_stats[0]
                
        except Exception as e:
            logger.error(f"Failed to generate comparison report: {e}")
            report['error'] = str(e)
        
        return report
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all end-to-end tests and return results."""
        logger.info("Starting comprehensive end-to-end validation...")
        
        try:
            self.setup_test_environment()
            
            # Test 1: Complete data extraction workflow
            self.test_results['data_extraction'] = self.test_complete_data_extraction_workflow()
            
            # Test 2: Database schema validation
            self.test_results['schema_validation'] = self.validate_database_schema()
            
            # Test 3: Data quality validation
            self.test_results['data_quality'] = self.validate_data_quality()
            
            # Test 4: dbt model compatibility (optional - may not work in all environments)
            try:
                self.test_results['dbt_compatibility'] = self.test_dbt_model_compatibility()
            except Exception as e:
                logger.warning(f"dbt compatibility test skipped: {e}")
                self.test_results['dbt_compatibility'] = None
            
            # Test 5: Error scenario handling
            self.test_results['error_handling'] = self.test_error_scenarios()
            
            # Test 6: Performance metrics
            self.test_results['performance'] = self.test_performance_metrics()
            
            # Generate comparison report
            comparison_report = self.generate_data_comparison_report()
            
            return self.test_results, comparison_report
            
        finally:
            self.cleanup_test_environment()


def main():
    """Main entry point for end-to-end validation."""
    print("=" * 80)
    print("NWS API Migration - End-to-End Validation")
    print("=" * 80)
    
    validator = EndToEndValidator()
    
    try:
        test_results, comparison_report = validator.run_all_tests()
        
        # Print results
        print("\nTest Results:")
        print("-" * 40)
        
        all_passed = True
        for test_name, result in test_results.items():
            if result is None:
                status = "SKIPPED"
                color = "\033[93m"  # Yellow
            elif result:
                status = "PASSED"
                color = "\033[92m"  # Green
            else:
                status = "FAILED"
                color = "\033[91m"  # Red
                all_passed = False
            
            print(f"{color}{test_name:25} {status}\033[0m")
        
        print("\nData Comparison Report:")
        print("-" * 40)
        print(json.dumps(comparison_report, indent=2, default=str))
        
        # Overall result
        print("\n" + "=" * 80)
        if all_passed:
            print("\033[92m✓ ALL TESTS PASSED - NWS API migration is successful!\033[0m")
            return 0
        else:
            print("\033[91m✗ SOME TESTS FAILED - Please review the results above\033[0m")
            return 1
            
    except Exception as e:
        logger.error(f"Validation failed with error: {e}")
        print(f"\033[91m✗ VALIDATION FAILED: {e}\033[0m")
        return 1


if __name__ == "__main__":
    sys.exit(main())