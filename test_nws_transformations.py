#!/usr/bin/env python3
"""
Test NWS data transformation functions.
Tests the transformation of NWS API responses to match existing schema.
"""
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any

# Add the extractor directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

# Import the transformation functions
from utils import (
    validate_nws_response,
    transform_nws_current_weather,
    transform_nws_hourly_forecast,
    transform_nws_daily_forecast,
    _convert_temperature,
    _convert_pressure,
    _convert_wind_speed,
    _map_nws_icon_to_weather_icon
)

# Mock NWS API responses for testing
MOCK_NWS_POINTS_RESPONSE = {
    "properties": {
        "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
        "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
        "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
    }
}

MOCK_NWS_CURRENT_RESPONSE = {
    "properties": {
        "timestamp": "2024-01-15T10:00:00+00:00",
        "temperature": {"value": 5.0, "unitCode": "wmoUnit:degC"},
        "relativeHumidity": {"value": 65},
        "windSpeed": {"value": 3.5, "unitCode": "wmoUnit:m_s-1"},
        "windDirection": {"value": 180},
        "barometricPressure": {"value": 101325, "unitCode": "wmoUnit:Pa"},
        "textDescription": "Partly Cloudy",
        "heatIndex": {"value": 6.0, "unitCode": "wmoUnit:degC"}
    }
}

MOCK_NWS_HOURLY_RESPONSE = {
    "properties": {
        "periods": [
            {
                "number": 1,
                "name": "This Hour",
                "startTime": "2024-01-15T10:00:00-05:00",
                "endTime": "2024-01-15T11:00:00-05:00",
                "isDaytime": True,
                "temperature": 41,
                "temperatureUnit": "F",
                "windSpeed": "10 mph",
                "windDirection": "NW",
                "shortForecast": "Partly Cloudy",
                "probabilityOfPrecipitation": {"value": 20}
            },
            {
                "number": 2,
                "name": "Next Hour",
                "startTime": "2024-01-15T11:00:00-05:00",
                "endTime": "2024-01-15T12:00:00-05:00",
                "isDaytime": True,
                "temperature": 43,
                "temperatureUnit": "F",
                "windSpeed": "12 mph",
                "windDirection": "N",
                "shortForecast": "Sunny",
                "probabilityOfPrecipitation": {"value": 10}
            }
        ]
    }
}

MOCK_NWS_DAILY_RESPONSE = {
    "properties": {
        "periods": [
            {
                "number": 1,
                "name": "Today",
                "startTime": "2024-01-15T06:00:00-05:00",
                "endTime": "2024-01-15T18:00:00-05:00",
                "isDaytime": True,
                "temperature": 45,
                "temperatureUnit": "F",
                "windSpeed": "10 to 15 mph",
                "windDirection": "NW",
                "shortForecast": "Partly Cloudy",
                "probabilityOfPrecipitation": {"value": 30}
            },
            {
                "number": 2,
                "name": "Tonight",
                "startTime": "2024-01-15T18:00:00-05:00",
                "endTime": "2024-01-16T06:00:00-05:00",
                "isDaytime": False,
                "temperature": 32,
                "temperatureUnit": "F",
                "windSpeed": "5 to 10 mph",
                "windDirection": "N",
                "shortForecast": "Clear",
                "probabilityOfPrecipitation": {"value": 10}
            },
            {
                "number": 3,
                "name": "Tomorrow",
                "startTime": "2024-01-16T06:00:00-05:00",
                "endTime": "2024-01-16T18:00:00-05:00",
                "isDaytime": True,
                "temperature": 48,
                "temperatureUnit": "F",
                "windSpeed": "8 mph",
                "windDirection": "NE",
                "shortForecast": "Sunny",
                "probabilityOfPrecipitation": {"value": 0}
            }
        ]
    }
}


def test_validation_functions():
    """Test NWS response validation functions."""
    print("Testing NWS response validation...")
    
    # Test points validation
    assert validate_nws_response(MOCK_NWS_POINTS_RESPONSE, 'points'), "Points validation should pass"
    assert not validate_nws_response({}, 'points'), "Empty points response should fail"
    print("✓ Points response validation tests passed")
    
    # Test current validation
    assert validate_nws_response(MOCK_NWS_CURRENT_RESPONSE, 'current'), "Current validation should pass"
    assert not validate_nws_response({"properties": {}}, 'current'), "Current response without temperature should fail"
    print("✓ Current response validation tests passed")
    
    # Test hourly validation
    assert validate_nws_response(MOCK_NWS_HOURLY_RESPONSE, 'hourly'), "Hourly validation should pass"
    assert not validate_nws_response({"properties": {}}, 'hourly'), "Hourly response without periods should fail"
    print("✓ Hourly response validation tests passed")
    
    # Test daily validation
    assert validate_nws_response(MOCK_NWS_DAILY_RESPONSE, 'daily'), "Daily validation should pass"
    assert not validate_nws_response({"properties": {"periods": "not_a_list"}}, 'daily'), "Daily response with invalid periods should fail"
    print("✓ Daily response validation tests passed")
    
    print("NWS response validation tests passed!\n")


def test_unit_conversion_functions():
    """Test unit conversion functions."""
    print("Testing unit conversion functions...")
    
    # Test temperature conversion
    assert _convert_temperature(5.0, "wmoUnit:degC") == 5.0, "Celsius to Celsius should be unchanged"
    assert abs(_convert_temperature(32.0, "wmoUnit:degF") - 0.0) < 0.1, "32°F should be 0°C"
    assert abs(_convert_temperature(273.15, "wmoUnit:K") - 0.0) < 0.1, "273.15K should be 0°C"
    assert _convert_temperature(None, "wmoUnit:degC") is None, "None temperature should return None"
    print("✓ Temperature conversion tests passed")
    
    # Test pressure conversion
    assert _convert_pressure(101325, "wmoUnit:Pa") == 1013, "101325 Pa should be 1013 hPa"
    assert _convert_pressure(1013, "wmoUnit:hPa") == 1013, "hPa to hPa should be unchanged"
    assert _convert_pressure(None, "wmoUnit:Pa") is None, "None pressure should return None"
    print("✓ Pressure conversion tests passed")
    
    # Test wind speed conversion
    assert _convert_wind_speed(10.0, "wmoUnit:m_s-1") == 10.0, "m/s to m/s should be unchanged"
    assert abs(_convert_wind_speed(36.0, "wmoUnit:km_h-1") - 10.0) < 0.1, "36 km/h should be 10 m/s"
    assert _convert_wind_speed(None, "wmoUnit:m_s-1") is None, "None wind speed should return None"
    print("✓ Wind speed conversion tests passed")
    
    print("Unit conversion tests passed!\n")


def test_icon_mapping():
    """Test weather description to icon mapping."""
    print("Testing icon mapping...")
    
    assert _map_nws_icon_to_weather_icon("Clear") == "01d", "Clear should map to 01d"
    assert _map_nws_icon_to_weather_icon("Partly Cloudy") == "02d", "Partly Cloudy should map to 02d"
    assert _map_nws_icon_to_weather_icon("Scattered Clouds") == "03d", "Scattered Clouds should map to 03d"
    assert _map_nws_icon_to_weather_icon("Overcast") == "04d", "Overcast should map to 04d"
    assert _map_nws_icon_to_weather_icon("Light Rain") == "09d", "Light Rain should map to 09d"
    assert _map_nws_icon_to_weather_icon("Rain") == "10d", "Rain should map to 10d"
    assert _map_nws_icon_to_weather_icon("Thunderstorm") == "11d", "Thunderstorm should map to 11d"
    assert _map_nws_icon_to_weather_icon("Snow") == "13d", "Snow should map to 13d"
    assert _map_nws_icon_to_weather_icon("Fog") == "50d", "Fog should map to 50d"
    assert _map_nws_icon_to_weather_icon("Unknown Weather") == "01d", "Unknown should default to 01d"
    
    print("✓ Icon mapping tests passed")
    print("Icon mapping tests passed!\n")


def test_current_weather_transformation():
    """Test current weather transformation."""
    print("Testing current weather transformation...")
    
    result = transform_nws_current_weather(MOCK_NWS_CURRENT_RESPONSE)
    
    assert result is not None, "Transformation should not return None"
    assert isinstance(result, dict), "Result should be a dictionary"
    
    # Check required fields
    required_fields = ['timestamp', 'temp', 'feels_like', 'humidity', 'pressure', 
                      'wind_speed', 'wind_deg', 'description', 'icon']
    for field in required_fields:
        assert field in result, f"Result should contain {field}"
    
    # Check specific values
    assert result['temp'] == 5.0, f"Temperature should be 5.0, got {result['temp']}"
    assert result['feels_like'] == 6.0, f"Feels like should be 6.0, got {result['feels_like']}"
    assert result['humidity'] == 65, f"Humidity should be 65, got {result['humidity']}"
    assert result['pressure'] == 1013, f"Pressure should be 1013, got {result['pressure']}"
    assert result['wind_speed'] == 3.5, f"Wind speed should be 3.5, got {result['wind_speed']}"
    assert result['wind_deg'] == 180, f"Wind direction should be 180, got {result['wind_deg']}"
    assert result['description'] == "Partly Cloudy", f"Description should be 'Partly Cloudy', got {result['description']}"
    assert result['icon'] == "02d", f"Icon should be '02d', got {result['icon']}"
    
    print("✓ Current weather transformation tests passed")
    
    # Test with invalid data
    invalid_result = transform_nws_current_weather({})
    assert invalid_result is None, "Invalid data should return None"
    
    print("Current weather transformation tests passed!\n")


def test_hourly_forecast_transformation():
    """Test hourly forecast transformation."""
    print("Testing hourly forecast transformation...")
    
    result = transform_nws_hourly_forecast(MOCK_NWS_HOURLY_RESPONSE)
    
    assert isinstance(result, list), "Result should be a list"
    assert len(result) == 2, f"Should have 2 periods, got {len(result)}"
    
    # Check first period
    first_period = result[0]
    required_fields = ['timestamp', 'temp', 'feels_like', 'humidity', 'pressure', 
                      'wind_speed', 'wind_deg', 'description', 'icon', 'pop']
    for field in required_fields:
        assert field in first_period, f"First period should contain {field}"
    
    # Check temperature conversion (41°F should be ~5°C)
    expected_temp = (41 - 32) * 5.0 / 9.0
    assert abs(first_period['temp'] - expected_temp) < 0.1, f"Temperature conversion incorrect: {first_period['temp']} vs {expected_temp}"
    
    # Check wind speed conversion (10 mph to m/s)
    expected_wind = 10 * 0.44704
    assert abs(first_period['wind_speed'] - expected_wind) < 0.1, f"Wind speed conversion incorrect: {first_period['wind_speed']} vs {expected_wind}"
    
    # Check wind direction (NW should be 315)
    assert first_period['wind_deg'] == 315, f"Wind direction should be 315, got {first_period['wind_deg']}"
    
    # Check probability of precipitation (20% should be 0.2)
    assert first_period['pop'] == 0.2, f"POP should be 0.2, got {first_period['pop']}"
    
    print("✓ Hourly forecast transformation tests passed")
    
    # Test with invalid data
    invalid_result = transform_nws_hourly_forecast({})
    assert invalid_result == [], "Invalid data should return empty list"
    
    print("Hourly forecast transformation tests passed!\n")


def test_daily_forecast_transformation():
    """Test daily forecast transformation."""
    print("Testing daily forecast transformation...")
    
    result = transform_nws_daily_forecast(MOCK_NWS_DAILY_RESPONSE)
    
    assert isinstance(result, list), "Result should be a list"
    assert len(result) >= 1, f"Should have at least 1 day, got {len(result)}"
    
    # Check first day
    first_day = result[0]
    required_fields = ['date', 'temp_min', 'temp_max', 'temp_day', 'temp_night', 
                      'humidity', 'pressure', 'wind_speed', 'wind_deg', 
                      'description', 'icon', 'pop']
    for field in required_fields:
        assert field in first_day, f"First day should contain {field}"
    
    # Check temperature conversions
    expected_day_temp = (45 - 32) * 5.0 / 9.0  # 45°F to Celsius
    expected_night_temp = (32 - 32) * 5.0 / 9.0  # 32°F to Celsius
    
    assert abs(first_day['temp_day'] - expected_day_temp) < 0.1, f"Day temperature incorrect: {first_day['temp_day']} vs {expected_day_temp}"
    assert abs(first_day['temp_night'] - expected_night_temp) < 0.1, f"Night temperature incorrect: {first_day['temp_night']} vs {expected_night_temp}"
    
    # Check that max POP is used (should be 0.3 from the day period)
    assert first_day['pop'] == 0.3, f"POP should be 0.3, got {first_day['pop']}"
    
    print("✓ Daily forecast transformation tests passed")
    
    # Test with invalid data
    invalid_result = transform_nws_daily_forecast({})
    assert invalid_result == [], "Invalid data should return empty list"
    
    print("Daily forecast transformation tests passed!\n")


def test_edge_cases():
    """Test edge cases and error handling."""
    print("Testing edge cases...")
    
    # Test with missing fields
    incomplete_current = {
        "properties": {
            "temperature": {"value": 10.0, "unitCode": "wmoUnit:degC"}
            # Missing other fields
        }
    }
    
    result = transform_nws_current_weather(incomplete_current)
    assert result is not None, "Should handle missing fields gracefully"
    assert result['temp'] == 10.0, "Should extract available temperature"
    assert result['humidity'] is None, "Missing humidity should be None"
    
    # Test with malformed temperature data
    malformed_current = {
        "properties": {
            "temperature": "not_a_dict",
            "textDescription": "Test"
        }
    }
    
    result = transform_nws_current_weather(malformed_current)
    assert result is not None, "Should handle malformed data"
    assert result['temp'] is None, "Malformed temperature should be None"
    
    print("✓ Edge case tests passed")
    print("Edge case tests passed!\n")


def main():
    """Run all transformation tests."""
    print("Running NWS Data Transformation Tests\n")
    
    try:
        test_validation_functions()
        test_unit_conversion_functions()
        test_icon_mapping()
        test_current_weather_transformation()
        test_hourly_forecast_transformation()
        test_daily_forecast_transformation()
        test_edge_cases()
        
        print("🎉 All transformation tests passed! NWS data transformation functions are working correctly.")
        print("\nTask 3 Implementation Summary:")
        print("✓ validate_nws_response() function for response structure validation")
        print("✓ transform_nws_current_weather() to map NWS current conditions to existing schema")
        print("✓ transform_nws_hourly_forecast() to convert NWS hourly data to existing format")
        print("✓ transform_nws_daily_forecast() to transform NWS daily forecasts")
        print("✓ Unit conversion functions (Pa to hPa for pressure, temperature mapping)")
        print("✓ Weather description to icon mapping")
        print("✓ Comprehensive error handling and edge case management")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)