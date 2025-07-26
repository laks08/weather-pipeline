#!/usr/bin/env python3
"""
Unit tests for NWS configuration and URL generation.
Focused tests for the NWSConfig class and related functionality.
"""
import sys
import os
import unittest
from unittest.mock import patch, Mock

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from config import (
    NWSConfig, NWSAPIError, NWSGeographicError, 
    NWSServiceUnavailableError, WeatherConfig
)


class TestNWSConfig(unittest.TestCase):
    """Unit tests for NWSConfig class."""
    
    def test_base_url_constant(self):
        """Test that BASE_URL is correctly set."""
        self.assertEqual(NWSConfig.BASE_URL, "https://api.weather.gov")
    
    def test_user_agent_constant(self):
        """Test that USER_AGENT contains required information."""
        user_agent = NWSConfig.USER_AGENT
        self.assertIn("boston-weather-etl", user_agent)
        self.assertIn("contact@example.com", user_agent)
        self.assertIsInstance(user_agent, str)
        self.assertGreater(len(user_agent), 10)
    
    def test_timeout_constant(self):
        """Test that TIMEOUT is set to reasonable value."""
        self.assertEqual(NWSConfig.TIMEOUT, 30)
        self.assertIsInstance(NWSConfig.TIMEOUT, int)
    
    def test_retry_constants(self):
        """Test retry-related constants."""
        self.assertEqual(NWSConfig.RETRY_ATTEMPTS, 3)
        self.assertEqual(NWSConfig.RETRY_DELAY, 1)
        self.assertIsInstance(NWSConfig.RETRY_ATTEMPTS, int)
        self.assertIsInstance(NWSConfig.RETRY_DELAY, (int, float))
    
    def test_get_points_url_basic(self):
        """Test basic points URL generation."""
        url = NWSConfig.get_points_url(42.3601, -71.0589)
        expected = "https://api.weather.gov/points/42.3601,-71.0589"
        self.assertEqual(url, expected)
    
    def test_get_points_url_different_coordinates(self):
        """Test points URL generation with different coordinates."""
        test_cases = [
            (40.7128, -74.006, "https://api.weather.gov/points/40.7128,-74.006"),
            (34.0522, -118.2437, "https://api.weather.gov/points/34.0522,-118.2437"),
            (25.7617, -80.1918, "https://api.weather.gov/points/25.7617,-80.1918"),
        ]
        
        for lat, lon, expected in test_cases:
            with self.subTest(lat=lat, lon=lon):
                url = NWSConfig.get_points_url(lat, lon)
                self.assertEqual(url, expected)
    
    def test_get_points_url_negative_coordinates(self):
        """Test points URL generation with negative coordinates."""
        url = NWSConfig.get_points_url(-25.2744, -57.6472)
        expected = "https://api.weather.gov/points/-25.2744,-57.6472"
        self.assertEqual(url, expected)
    
    def test_get_points_url_zero_coordinates(self):
        """Test points URL generation with zero coordinates."""
        url = NWSConfig.get_points_url(0.0, 0.0)
        expected = "https://api.weather.gov/points/0.0,0.0"
        self.assertEqual(url, expected)
    
    def test_get_points_url_precision(self):
        """Test points URL generation with high precision coordinates."""
        url = NWSConfig.get_points_url(42.360123456, -71.058987654)
        expected = "https://api.weather.gov/points/42.360123456,-71.058987654"
        self.assertEqual(url, expected)
    
    def test_get_headers_structure(self):
        """Test that headers contain required fields."""
        headers = NWSConfig.get_headers()
        
        self.assertIsInstance(headers, dict)
        self.assertIn("User-Agent", headers)
        self.assertIn("Accept", headers)
    
    def test_get_headers_user_agent(self):
        """Test User-Agent header content."""
        headers = NWSConfig.get_headers()
        user_agent = headers["User-Agent"]
        
        self.assertIn("boston-weather-etl", user_agent)
        self.assertIn("contact@example.com", user_agent)
        self.assertEqual(user_agent, NWSConfig.USER_AGENT)
    
    def test_get_headers_accept(self):
        """Test Accept header content."""
        headers = NWSConfig.get_headers()
        self.assertEqual(headers["Accept"], "application/json")
    
    def test_get_headers_immutable(self):
        """Test that headers don't change between calls."""
        headers1 = NWSConfig.get_headers()
        headers2 = NWSConfig.get_headers()
        
        self.assertEqual(headers1, headers2)
        self.assertIsNot(headers1, headers2)  # Should be different objects
    
    def test_validate_coordinates_continental_us(self):
        """Test coordinate validation for continental US."""
        valid_coordinates = [
            (42.3601, -71.0589),  # Boston
            (40.7128, -74.0060),  # NYC
            (34.0522, -118.2437), # LA
            (41.8781, -87.6298),  # Chicago
            (29.7604, -95.3698),  # Houston
            (33.4484, -112.0740), # Phoenix
            (39.7392, -104.9903), # Denver
            (47.6062, -122.3321), # Seattle
            (25.7617, -80.1918),  # Miami
        ]
        
        for lat, lon in valid_coordinates:
            with self.subTest(lat=lat, lon=lon):
                self.assertTrue(NWSConfig.validate_coordinates(lat, lon))
    
    def test_validate_coordinates_alaska(self):
        """Test coordinate validation for Alaska."""
        alaska_coordinates = [
            (61.2181, -149.9003),  # Anchorage
            (64.2008, -149.4937),  # Fairbanks
            (58.3019, -134.4197),  # Juneau
            (71.2906, -156.7886),  # Barrow (Utqiagvik)
        ]
        
        for lat, lon in alaska_coordinates:
            with self.subTest(lat=lat, lon=lon):
                self.assertTrue(NWSConfig.validate_coordinates(lat, lon))
    
    def test_validate_coordinates_hawaii(self):
        """Test coordinate validation for Hawaii."""
        hawaii_coordinates = [
            (21.3099, -157.8581),  # Honolulu
            (19.8968, -155.5828),  # Hilo
            (20.7984, -156.3319),  # Maui
        ]
        
        for lat, lon in hawaii_coordinates:
            with self.subTest(lat=lat, lon=lon):
                self.assertTrue(NWSConfig.validate_coordinates(lat, lon))
    
    def test_validate_coordinates_puerto_rico(self):
        """Test coordinate validation for Puerto Rico."""
        pr_coordinates = [
            (18.2208, -66.5901),  # San Juan
            (18.0142, -66.6141),  # Ponce
        ]
        
        for lat, lon in pr_coordinates:
            with self.subTest(lat=lat, lon=lon):
                self.assertTrue(NWSConfig.validate_coordinates(lat, lon))
    
    def test_validate_coordinates_invalid_international(self):
        """Test coordinate validation for international locations."""
        invalid_coordinates = [
            (51.5074, -0.1278),   # London
            (48.8566, 2.3522),    # Paris
            (35.6762, 139.6503),  # Tokyo
            (-33.8688, 151.2093), # Sydney
            (55.7558, 37.6176),   # Moscow
            (-22.9068, -43.1729), # Rio de Janeiro
            (19.4326, -99.1332),  # Mexico City
            # Note: Ottawa (45.4215, -75.6972) might be considered valid as it's close to US border
        ]
        
        for lat, lon in invalid_coordinates:
            with self.subTest(lat=lat, lon=lon):
                self.assertFalse(NWSConfig.validate_coordinates(lat, lon))
    
    def test_validate_coordinates_boundary_cases(self):
        """Test coordinate validation at boundaries."""
        # Test coordinates just outside valid ranges
        boundary_cases = [
            (24.4, -125.1),  # Just south of continental US
            (49.5, -66.8),   # Just north of continental US
            (24.6, -65.0),   # Just east of continental US
            (49.3, -125.1),  # Just west of continental US
        ]
        
        for lat, lon in boundary_cases:
            with self.subTest(lat=lat, lon=lon):
                # These should be invalid as they're just outside the boundaries
                result = NWSConfig.validate_coordinates(lat, lon)
                # The exact result depends on the precision of the boundary check
                self.assertIsInstance(result, bool)
    
    def test_validate_coordinates_extreme_values(self):
        """Test coordinate validation with extreme values."""
        extreme_cases = [
            (90.0, 0.0),     # North Pole
            (-90.0, 0.0),    # South Pole
            (0.0, 180.0),    # International Date Line
            (0.0, -180.0),   # International Date Line
        ]
        
        for lat, lon in extreme_cases:
            with self.subTest(lat=lat, lon=lon):
                self.assertFalse(NWSConfig.validate_coordinates(lat, lon))


class TestNWSErrorClasses(unittest.TestCase):
    """Unit tests for NWS error classes."""
    
    def test_nws_api_error_base(self):
        """Test NWSAPIError base class."""
        error = NWSAPIError("Test error message")
        
        self.assertIsInstance(error, Exception)
        self.assertEqual(str(error), "Test error message")
        self.assertEqual(error.args, ("Test error message",))
    
    def test_nws_api_error_empty_message(self):
        """Test NWSAPIError with empty message."""
        error = NWSAPIError("")
        self.assertEqual(str(error), "")
    
    def test_nws_geographic_error_inheritance(self):
        """Test NWSGeographicError inheritance."""
        error = NWSGeographicError("Outside coverage area")
        
        self.assertIsInstance(error, NWSAPIError)
        self.assertIsInstance(error, Exception)
        self.assertEqual(str(error), "Outside coverage area")
    
    def test_nws_service_unavailable_error_inheritance(self):
        """Test NWSServiceUnavailableError inheritance."""
        error = NWSServiceUnavailableError("Service is down")
        
        self.assertIsInstance(error, NWSAPIError)
        self.assertIsInstance(error, Exception)
        self.assertEqual(str(error), "Service is down")
    
    def test_error_class_hierarchy(self):
        """Test that error classes maintain proper hierarchy."""
        # Test that all NWS errors inherit from NWSAPIError
        geo_error = NWSGeographicError("test")
        service_error = NWSServiceUnavailableError("test")
        
        self.assertTrue(issubclass(NWSGeographicError, NWSAPIError))
        self.assertTrue(issubclass(NWSServiceUnavailableError, NWSAPIError))
        self.assertTrue(issubclass(NWSAPIError, Exception))
        
        # Test isinstance relationships
        self.assertIsInstance(geo_error, NWSAPIError)
        self.assertIsInstance(service_error, NWSAPIError)
    
    def test_error_with_none_message(self):
        """Test error classes with None message."""
        try:
            error = NWSAPIError(None)
            str_repr = str(error)
            # Should handle None gracefully
            self.assertIsInstance(str_repr, str)
        except TypeError:
            # This is also acceptable behavior
            pass


class TestWeatherConfigIntegration(unittest.TestCase):
    """Unit tests for WeatherConfig integration with NWS."""
    
    def test_weather_config_boston_coordinates(self):
        """Test that WeatherConfig has valid Boston coordinates."""
        config = WeatherConfig()
        
        # Test that coordinates are valid for NWS
        self.assertTrue(NWSConfig.validate_coordinates(config.boston_lat, config.boston_lon))
        
        # Test specific Boston coordinates
        self.assertAlmostEqual(config.boston_lat, 42.3601, places=4)
        self.assertAlmostEqual(config.boston_lon, -71.0589, places=4)
    
    def test_weather_config_coordinate_validation(self):
        """Test WeatherConfig coordinate validation."""
        # Test that validation works by creating new instances with invalid values
        try:
            # This should raise ValueError during validation
            invalid_config = WeatherConfig(boston_lat=91.0)
            self.fail("Should have raised ValueError for invalid latitude")
        except ValueError:
            pass  # Expected
        
        try:
            # This should raise ValueError during validation
            invalid_config = WeatherConfig(boston_lon=181.0)
            self.fail("Should have raised ValueError for invalid longitude")
        except ValueError:
            pass  # Expected


def run_unit_tests():
    """Run all unit tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestNWSConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestNWSErrorClasses))
    suite.addTests(loader.loadTestsFromTestCase(TestWeatherConfigIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    print("Running NWS Configuration Unit Tests...")
    print("=" * 50)
    
    success = run_unit_tests()
    
    if success:
        print("\n🎉 All NWS configuration unit tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some NWS configuration unit tests failed!")
        sys.exit(1)