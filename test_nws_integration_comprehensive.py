#!/usr/bin/env python3
"""
Comprehensive test suite for NWS integration.
Tests all aspects of the NWS API integration including configuration,
data transformation, error handling, and complete workflow.
"""
import sys
import os
import time
import json
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, Mock
import pytest

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from config import (
    NWSConfig, NWSAPIError, NWSGeographicError, 
    NWSServiceUnavailableError, WeatherConfig
)
from nws_client import NWSAPIClient, handle_nws_error, retry_with_exponential_backoff
from nws_cache import NWSCache
from utils import (
    validate_nws_response, transform_nws_current_weather,
    transform_nws_hourly_forecast, transform_nws_daily_forecast,
    _convert_temperature, _convert_pressure, _convert_wind_speed,
    _map_nws_icon_to_weather_icon
)
from main import WeatherExtractor


class TestNWSConfiguration:
    """Test suite for NWS configuration and URL generation."""
    
    def test_nws_config_constants(self):
        """Test NWS configuration constants."""
        assert NWSConfig.BASE_URL == "https://api.weather.gov"
        assert NWSConfig.TIMEOUT == 30
        assert NWSConfig.RETRY_ATTEMPTS == 3
        assert NWSConfig.RETRY_DELAY == 1
        assert "contact@example.com" in NWSConfig.USER_AGENT
    
    def test_points_url_generation(self):
        """Test Points API URL generation."""
        # Test Boston coordinates
        url = NWSConfig.get_points_url(42.3601, -71.0589)
        expected = "https://api.weather.gov/points/42.3601,-71.0589"
        assert url == expected
        
        # Test with different coordinates
        url = NWSConfig.get_points_url(40.7128, -74.0060)
        expected = "https://api.weather.gov/points/40.7128,-74.0060"
        assert url == expected
        
        # Test with negative coordinates
        url = NWSConfig.get_points_url(-25.2744, -57.6472)
        expected = "https://api.weather.gov/points/-25.2744,-57.6472"
        assert url == expected
    
    def test_headers_generation(self):
        """Test HTTP headers generation."""
        headers = NWSConfig.get_headers()
        
        assert "User-Agent" in headers
        assert "Accept" in headers
        assert headers["Accept"] == "application/json"
        assert "contact@example.com" in headers["User-Agent"]
        assert "boston-weather-etl" in headers["User-Agent"]
    
    def test_coordinate_validation(self):
        """Test coordinate validation for NWS coverage."""
        # Valid US coordinates
        assert NWSConfig.validate_coordinates(42.3601, -71.0589)  # Boston
        assert NWSConfig.validate_coordinates(40.7128, -74.0060)  # NYC
        assert NWSConfig.validate_coordinates(34.0522, -118.2437)  # LA
        assert NWSConfig.validate_coordinates(61.2181, -149.9003)  # Anchorage, AK
        assert NWSConfig.validate_coordinates(21.3099, -157.8581)  # Honolulu, HI
        assert NWSConfig.validate_coordinates(18.2208, -66.5901)  # Puerto Rico
        
        # Invalid coordinates (outside US)
        assert not NWSConfig.validate_coordinates(51.5074, -0.1278)  # London
        assert not NWSConfig.validate_coordinates(48.8566, 2.3522)   # Paris
        assert not NWSConfig.validate_coordinates(-33.8688, 151.2093)  # Sydney
        assert not NWSConfig.validate_coordinates(35.6762, 139.6503)  # Tokyo
    
    def test_error_classes_inheritance(self):
        """Test NWS error classes inheritance."""
        # Test base error
        error = NWSAPIError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"
        
        # Test geographic error
        geo_error = NWSGeographicError("Outside coverage")
        assert isinstance(geo_error, NWSAPIError)
        assert isinstance(geo_error, Exception)
        
        # Test service unavailable error
        service_error = NWSServiceUnavailableError("Service down")
        assert isinstance(service_error, NWSAPIError)
        assert isinstance(service_error, Exception)


class TestNWSDataTransformation:
    """Test suite for NWS data transformation functions."""
    
    @pytest.fixture
    def mock_nws_current_response(self):
        """Mock NWS current conditions response."""
        return {
            "properties": {
                "timestamp": "2024-01-15T10:00:00+00:00",
                "temperature": {"value": 5.0, "unitCode": "wmoUnit:degC"},
                "heatIndex": {"value": 7.0, "unitCode": "wmoUnit:degC"},
                "relativeHumidity": {"value": 65},
                "barometricPressure": {"value": 101325, "unitCode": "wmoUnit:Pa"},
                "windSpeed": {"value": 3.5, "unitCode": "wmoUnit:m_s-1"},
                "windDirection": {"value": 180},
                "textDescription": "Partly Cloudy"
            }
        }
    
    @pytest.fixture
    def mock_nws_hourly_response(self):
        """Mock NWS hourly forecast response."""
        return {
            "properties": {
                "periods": [
                    {
                        "startTime": "2024-01-15T10:00:00+00:00",
                        "temperature": 41,
                        "temperatureUnit": "F",
                        "windSpeed": "10 mph",
                        "windDirection": "NW",
                        "shortForecast": "Partly Cloudy",
                        "probabilityOfPrecipitation": {"value": 20}
                    },
                    {
                        "startTime": "2024-01-15T11:00:00+00:00",
                        "temperature": 43,
                        "temperatureUnit": "F",
                        "windSpeed": "12 mph",
                        "windDirection": "N",
                        "shortForecast": "Mostly Sunny",
                        "probabilityOfPrecipitation": {"value": 10}
                    }
                ]
            }
        }
    
    @pytest.fixture
    def mock_nws_daily_response(self):
        """Mock NWS daily forecast response."""
        return {
            "properties": {
                "periods": [
                    {
                        "startTime": "2024-01-15T06:00:00+00:00",
                        "temperature": 50,
                        "temperatureUnit": "F",
                        "isDaytime": True,
                        "windSpeed": "15 mph",
                        "windDirection": "SW",
                        "shortForecast": "Sunny",
                        "probabilityOfPrecipitation": {"value": 0}
                    },
                    {
                        "startTime": "2024-01-15T18:00:00+00:00",
                        "temperature": 35,
                        "temperatureUnit": "F",
                        "isDaytime": False,
                        "windSpeed": "8 mph",
                        "windDirection": "W",
                        "shortForecast": "Clear",
                        "probabilityOfPrecipitation": {"value": 0}
                    }
                ]
            }
        }
    
    def test_validate_nws_response(self):
        """Test NWS response validation."""
        # Test valid points response
        points_response = {
            "properties": {
                "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
                "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly"
            }
        }
        assert validate_nws_response(points_response, 'points')
        
        # Test invalid points response (missing properties)
        invalid_points = {"invalid": "data"}
        assert not validate_nws_response(invalid_points, 'points')
        
        # Test valid current response
        current_response = {
            "properties": {
                "temperature": {"value": 20.0, "unitCode": "wmoUnit:degC"}
            }
        }
        assert validate_nws_response(current_response, 'current')
        
        # Test invalid current response
        invalid_current = {"properties": {}}
        assert not validate_nws_response(invalid_current, 'current')
        
        # Test valid forecast response
        forecast_response = {
            "properties": {
                "periods": [{"startTime": "2024-01-15T10:00:00+00:00"}]
            }
        }
        assert validate_nws_response(forecast_response, 'hourly')
        assert validate_nws_response(forecast_response, 'daily')
        
        # Test invalid forecast response
        invalid_forecast = {"properties": {"periods": "not_a_list"}}
        assert not validate_nws_response(invalid_forecast, 'hourly')
    
    def test_temperature_conversion(self):
        """Test temperature conversion functions."""
        # Test Celsius (no conversion)
        assert _convert_temperature(20.0, "wmoUnit:degC") == 20.0
        
        # Test Fahrenheit to Celsius
        fahrenheit_temp = _convert_temperature(68.0, "wmoUnit:degF")
        assert abs(fahrenheit_temp - 20.0) < 0.1
        
        # Test Kelvin to Celsius
        kelvin_temp = _convert_temperature(293.15, "wmoUnit:K")
        assert abs(kelvin_temp - 20.0) < 0.1
        
        # Test None value
        assert _convert_temperature(None, "wmoUnit:degC") is None
        
        # Test invalid value
        assert _convert_temperature("invalid", "wmoUnit:degC") is None
    
    def test_pressure_conversion(self):
        """Test pressure conversion functions."""
        # Test Pascals to hPa
        assert _convert_pressure(101325, "wmoUnit:Pa") == 1013
        
        # Test hPa (no conversion)
        assert _convert_pressure(1013, "wmoUnit:hPa") == 1013
        
        # Test None value
        assert _convert_pressure(None, "wmoUnit:Pa") is None
        
        # Test invalid value
        assert _convert_pressure("invalid", "wmoUnit:Pa") is None
    
    def test_wind_speed_conversion(self):
        """Test wind speed conversion functions."""
        # Test m/s (no conversion)
        assert _convert_wind_speed(10.0, "wmoUnit:m_s-1") == 10.0
        
        # Test km/h to m/s
        kmh_speed = _convert_wind_speed(36.0, "wmoUnit:km_h-1")
        assert abs(kmh_speed - 10.0) < 0.1
        
        # Test mph to m/s
        mph_speed = _convert_wind_speed(22.369, "wmoUnit:mi_h-1")
        assert abs(mph_speed - 10.0) < 0.1
        
        # Test None value
        assert _convert_wind_speed(None, "wmoUnit:m_s-1") is None
    
    def test_icon_mapping(self):
        """Test weather description to icon mapping."""
        # Test clear conditions
        assert _map_nws_icon_to_weather_icon("Clear") == "01d"
        assert _map_nws_icon_to_weather_icon("Sunny") == "01d"
        
        # Test cloudy conditions
        assert _map_nws_icon_to_weather_icon("Few Clouds") == "02d"
        assert _map_nws_icon_to_weather_icon("Partly Cloudy") == "02d"
        assert _map_nws_icon_to_weather_icon("Scattered Clouds") == "03d"
        assert _map_nws_icon_to_weather_icon("Broken Clouds") == "04d"
        assert _map_nws_icon_to_weather_icon("Overcast") == "04d"
        
        # Test precipitation
        assert _map_nws_icon_to_weather_icon("Light Rain") == "09d"
        assert _map_nws_icon_to_weather_icon("Rain") == "10d"
        assert _map_nws_icon_to_weather_icon("Thunderstorm") == "11d"
        assert _map_nws_icon_to_weather_icon("Snow") == "13d"
        
        # Test fog/mist
        assert _map_nws_icon_to_weather_icon("Fog") == "50d"
        assert _map_nws_icon_to_weather_icon("Mist") == "50d"
        
        # Test unknown/empty
        assert _map_nws_icon_to_weather_icon("") == "01d"
        assert _map_nws_icon_to_weather_icon("Unknown Weather") == "01d"
    
    def test_transform_current_weather(self, mock_nws_current_response):
        """Test current weather transformation."""
        result = transform_nws_current_weather(mock_nws_current_response)
        
        assert result is not None
        assert result['temp'] == 5.0
        assert result['feels_like'] == 7.0  # Heat index used
        assert result['humidity'] == 65
        assert result['pressure'] == 1013  # Converted from Pa to hPa
        assert result['wind_speed'] == 3.5
        assert result['wind_deg'] == 180
        assert result['description'] == "Partly Cloudy"
        assert result['icon'] == "02d"
        assert isinstance(result['timestamp'], datetime)
    
    def test_transform_current_weather_invalid(self):
        """Test current weather transformation with invalid data."""
        # Test with invalid response
        result = transform_nws_current_weather({"invalid": "data"})
        assert result is None
        
        # Test with missing properties
        result = transform_nws_current_weather({"properties": {}})
        assert result is not None  # Should still create basic structure
    
    def test_transform_hourly_forecast(self, mock_nws_hourly_response):
        """Test hourly forecast transformation."""
        result = transform_nws_hourly_forecast(mock_nws_hourly_response)
        
        assert len(result) == 2
        
        # Test first period
        first_period = result[0]
        assert abs(first_period['temp'] - 5.0) < 0.1  # 41F to C
        assert first_period['feels_like'] == first_period['temp']  # Should match temp
        assert abs(first_period['wind_speed'] - 4.47) < 0.1  # 10 mph to m/s
        assert first_period['wind_deg'] == 315  # NW
        assert first_period['description'] == "Partly Cloudy"
        assert first_period['pop'] == 0.2  # 20% to decimal
        assert isinstance(first_period['timestamp'], datetime)
    
    def test_transform_daily_forecast(self, mock_nws_daily_response):
        """Test daily forecast transformation."""
        result = transform_nws_daily_forecast(mock_nws_daily_response)
        
        assert len(result) == 1  # Should group day/night into one daily record
        
        daily_record = result[0]
        assert abs(daily_record['temp_day'] - 10.0) < 0.1  # 50F to C
        assert abs(daily_record['temp_night'] - 1.67) < 0.1  # 35F to C
        assert daily_record['temp_max'] == daily_record['temp_day']
        assert daily_record['temp_min'] == daily_record['temp_night']
        assert daily_record['description'] == "Sunny"  # Day description used
        assert daily_record['pop'] == 0.0


class TestNWSErrorHandling:
    """Test suite for NWS error handling scenarios."""
    
    def test_handle_nws_error_404(self):
        """Test handling of 404 errors (geographic)."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        with pytest.raises(NWSGeographicError):
            handle_nws_error(mock_response)
    
    def test_handle_nws_error_503(self):
        """Test handling of 503 errors (service unavailable)."""
        mock_response = Mock()
        mock_response.status_code = 503
        
        with pytest.raises(NWSServiceUnavailableError):
            handle_nws_error(mock_response)
    
    def test_handle_nws_error_500(self):
        """Test handling of 500 errors (internal server error)."""
        mock_response = Mock()
        mock_response.status_code = 500
        
        with pytest.raises(NWSServiceUnavailableError):
            handle_nws_error(mock_response)
    
    def test_handle_nws_error_429(self):
        """Test handling of 429 errors (rate limit)."""
        mock_response = Mock()
        mock_response.status_code = 429
        
        with pytest.raises(NWSServiceUnavailableError):
            handle_nws_error(mock_response)
    
    def test_handle_nws_error_generic(self):
        """Test handling of generic errors."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.ok = False
        mock_response.text = "Bad Request"
        
        with pytest.raises(NWSAPIError):
            handle_nws_error(mock_response)
    
    def test_retry_decorator_success(self):
        """Test retry decorator with successful call."""
        call_count = 0
        
        @retry_with_exponential_backoff(max_attempts=3, base_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = test_function()
        assert result == "success"
        assert call_count == 1
    
    def test_retry_decorator_eventual_success(self):
        """Test retry decorator with eventual success."""
        call_count = 0
        
        @retry_with_exponential_backoff(max_attempts=3, base_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NWSServiceUnavailableError("Service down")
            return "success"
        
        result = test_function()
        assert result == "success"
        assert call_count == 3
    
    def test_retry_decorator_max_attempts(self):
        """Test retry decorator reaching max attempts."""
        call_count = 0
        
        @retry_with_exponential_backoff(max_attempts=2, base_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise NWSServiceUnavailableError("Service down")
        
        with pytest.raises(NWSServiceUnavailableError):
            test_function()
        
        assert call_count == 2
    
    def test_retry_decorator_no_retry_geographic(self):
        """Test retry decorator doesn't retry geographic errors."""
        call_count = 0
        
        @retry_with_exponential_backoff(max_attempts=3, base_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise NWSGeographicError("Outside coverage")
        
        with pytest.raises(NWSGeographicError):
            test_function()
        
        assert call_count == 1  # Should not retry


class TestNWSCache:
    """Test suite for NWS caching functionality."""
    
    def test_cache_basic_operations(self):
        """Test basic cache operations."""
        cache = NWSCache(cache_ttl=3600)
        
        # Test cache miss
        result = cache.get_cached_points(42.3601, -71.0589)
        assert result is None
        
        # Test cache store and hit
        test_data = {"properties": {"forecast": "test_url"}}
        cache.cache_points_data(42.3601, -71.0589, test_data)
        
        result = cache.get_cached_points(42.3601, -71.0589)
        assert result is not None
        assert result["properties"]["forecast"] == "test_url"
        assert "_cached_at" not in result  # Internal field should be filtered
    
    def test_cache_expiration(self):
        """Test cache expiration."""
        cache = NWSCache(cache_ttl=1)  # 1 second TTL
        
        test_data = {"properties": {"forecast": "test_url"}}
        cache.cache_points_data(42.3601, -71.0589, test_data)
        
        # Should be available immediately
        result = cache.get_cached_points(42.3601, -71.0589)
        assert result is not None
        
        # Wait for expiration
        time.sleep(2)
        
        # Should be expired
        result = cache.get_cached_points(42.3601, -71.0589)
        assert result is None
    
    def test_cache_cleanup(self):
        """Test cache cleanup of expired entries."""
        cache = NWSCache(cache_ttl=1)
        
        # Add multiple entries
        test_data = {"properties": {"forecast": "test_url"}}
        cache.cache_points_data(42.3601, -71.0589, test_data)
        cache.cache_points_data(40.7128, -74.0060, test_data)
        
        # Wait for expiration
        time.sleep(2)
        
        # Add fresh entry
        cache.cache_points_data(34.0522, -118.2437, test_data)
        
        # Cleanup should remove expired entries
        removed_count = cache.cleanup_expired()
        assert removed_count == 2
        
        # Verify only fresh entry remains
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 1
    
    def test_cache_stats(self):
        """Test cache statistics."""
        cache = NWSCache(cache_ttl=3600)
        
        # Empty cache stats
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 0
        assert stats["valid_entries"] == 0
        assert stats["cache_ttl_seconds"] == 3600
        
        # Add entries
        test_data = {"properties": {"forecast": "test_url"}}
        cache.cache_points_data(42.3601, -71.0589, test_data)
        cache.cache_points_data(40.7128, -74.0060, test_data)
        
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 2
        assert stats["valid_entries"] == 2


class TestNWSIntegration:
    """Test suite for complete NWS API workflow integration."""
    
    @pytest.fixture
    def mock_points_response(self):
        """Mock points API response."""
        return {
            "properties": {
                "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
                "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
                "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
            }
        }
    
    @pytest.fixture
    def mock_stations_response(self):
        """Mock observation stations response."""
        return {
            "features": [
                {
                    "properties": {
                        "stationIdentifier": "KBOS"
                    }
                }
            ]
        }
    
    def test_nws_client_initialization(self):
        """Test NWS API client initialization."""
        client = NWSAPIClient(cache_ttl=3600)
        
        assert client.config is not None
        assert client.session is not None
        assert client.cache is not None
        assert "User-Agent" in client.session.headers
        
        client.close()
    
    @patch('requests.Session.get')
    def test_nws_client_make_request_success(self, mock_get):
        """Test successful NWS API request."""
        # Mock successful response
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"test": "data"}
        mock_get.return_value = mock_response
        
        client = NWSAPIClient()
        result = client.make_request("https://api.weather.gov/test")
        
        assert result == {"test": "data"}
        mock_get.assert_called_once()
        client.close()
    
    @patch('requests.Session.get')
    def test_nws_client_make_request_error(self, mock_get):
        """Test NWS API request with error response."""
        # Mock error response
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        client = NWSAPIClient()
        
        with pytest.raises(NWSGeographicError):
            client.make_request("https://api.weather.gov/test")
        
        client.close()
    
    def test_weather_extractor_initialization(self):
        """Test WeatherExtractor initialization."""
        with patch('extractor.main.DatabaseManager'):
            extractor = WeatherExtractor()
            
            assert extractor.lat == 42.3601
            assert extractor.lon == -71.0589
            assert extractor.nws_cache is not None
    
    def test_weather_extractor_invalid_coordinates(self):
        """Test WeatherExtractor with invalid coordinates."""
        with patch('extractor.config.config') as mock_config:
            mock_config.boston_lat = 51.5074  # London
            mock_config.boston_lon = -0.1278
            mock_config.duckdb_path = "/tmp/test.db"
            
            with patch('extractor.main.DatabaseManager'):
                with pytest.raises(NWSGeographicError):
                    WeatherExtractor()
    
    @patch('extractor.main.DatabaseManager')
    @patch.object(WeatherExtractor, '_make_nws_request')
    def test_complete_workflow_success(self, mock_request, mock_db):
        """Test complete weather extraction workflow."""
        # Mock responses
        points_response = {
            "properties": {
                "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
                "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
                "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
            }
        }
        
        stations_response = {
            "features": [
                {"properties": {"stationIdentifier": "KBOS"}}
            ]
        }
        
        current_response = {
            "properties": {
                "timestamp": "2024-01-15T10:00:00+00:00",
                "temperature": {"value": 5.0, "unitCode": "wmoUnit:degC"},
                "relativeHumidity": {"value": 65},
                "textDescription": "Clear"
            }
        }
        
        hourly_response = {
            "properties": {
                "periods": [
                    {
                        "startTime": "2024-01-15T10:00:00+00:00",
                        "temperature": 41,
                        "temperatureUnit": "F",
                        "windSpeed": "10 mph",
                        "windDirection": "NW",
                        "shortForecast": "Clear"
                    }
                ]
            }
        }
        
        daily_response = {
            "properties": {
                "periods": [
                    {
                        "startTime": "2024-01-15T06:00:00+00:00",
                        "temperature": 50,
                        "temperatureUnit": "F",
                        "isDaytime": True,
                        "shortForecast": "Sunny"
                    }
                ]
            }
        }
        
        # Configure mock to return different responses based on URL
        def mock_request_side_effect(url):
            if "points" in url:
                return points_response
            elif "stations" in url and not "observations" in url:
                return stations_response
            elif "observations" in url:
                return current_response
            elif "forecastHourly" in url:
                return hourly_response
            elif "forecast" in url:
                return daily_response
            return None
        
        mock_request.side_effect = mock_request_side_effect
        
        # Create extractor and run workflow
        extractor = WeatherExtractor()
        
        # Mock database manager context
        mock_db_instance = Mock()
        mock_db.return_value.__enter__.return_value = mock_db_instance
        
        # Run extraction
        extractor.extract_and_store_weather_data()
        
        # Verify database calls were made
        mock_db_instance.insert_current_weather.assert_called_once()
        mock_db_instance.insert_hourly_weather.assert_called_once()
        mock_db_instance.insert_daily_weather.assert_called_once()


def run_tests():
    """Run all tests using pytest."""
    import subprocess
    import sys
    
    # Run pytest on this file
    result = subprocess.run([
        sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0


if __name__ == "__main__":
    # If pytest is not available, run basic tests
    try:
        import pytest
        success = run_tests()
    except ImportError:
        print("pytest not available, running basic tests...")
        success = True
        
        try:
            # Run basic tests without pytest
            test_config = TestNWSConfiguration()
            test_config.test_nws_config_constants()
            test_config.test_points_url_generation()
            test_config.test_headers_generation()
            test_config.test_coordinate_validation()
            test_config.test_error_classes_inheritance()
            print("✓ Configuration tests passed")
            
            test_transform = TestNWSDataTransformation()
            test_transform.test_validate_nws_response()
            test_transform.test_temperature_conversion()
            test_transform.test_pressure_conversion()
            test_transform.test_wind_speed_conversion()
            test_transform.test_icon_mapping()
            print("✓ Data transformation tests passed")
            
            test_error = TestNWSErrorHandling()
            test_error.test_retry_decorator_success()
            print("✓ Error handling tests passed")
            
            test_cache = TestNWSCache()
            test_cache.test_cache_basic_operations()
            print("✓ Cache tests passed")
            
            print("\n🎉 All basic tests passed!")
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            success = False
    
    sys.exit(0 if success else 1)