#!/usr/bin/env python3
"""
Test script to verify NWS cache integration works correctly.
"""
import sys
import os
import time
import json
from unittest.mock import patch, MagicMock

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from nws_cache import NWSCache
from config import NWSConfig

def test_nws_cache_basic_functionality():
    """Test basic cache functionality."""
    print("Testing NWS cache basic functionality...")
    
    # Create cache instance
    cache = NWSCache(cache_ttl=2)  # 2 seconds for testing
    
    # Test coordinates
    lat, lon = 42.3601, -71.0589  # Boston
    
    # Test cache miss
    result = cache.get_cached_points(lat, lon)
    assert result is None, "Cache should be empty initially"
    print("✓ Cache miss test passed")
    
    # Test cache storage
    test_data = {
        "properties": {
            "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
            "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
            "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
        }
    }
    
    cache.cache_points_data(lat, lon, test_data)
    print("✓ Cache storage test passed")
    
    # Test cache hit
    result = cache.get_cached_points(lat, lon)
    assert result is not None, "Cache should return data"
    assert result["properties"]["forecast"] == test_data["properties"]["forecast"]
    assert "_cached_at" not in result, "Internal timestamp should not be returned"
    print("✓ Cache hit test passed")
    
    # Test cache expiration
    print("Waiting for cache to expire...")
    time.sleep(3)  # Wait for expiration
    result = cache.get_cached_points(lat, lon)
    assert result is None, "Cache should be expired"
    print("✓ Cache expiration test passed")
    
    print("All basic cache tests passed!")

def test_cache_stats():
    """Test cache statistics functionality."""
    print("\nTesting cache statistics...")
    
    cache = NWSCache(cache_ttl=3600)
    
    # Initial stats
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 0
    assert stats["valid_entries"] == 0
    assert stats["cache_ttl_seconds"] == 3600
    print("✓ Initial stats test passed")
    
    # Add some data
    test_data = {"properties": {"test": "data"}}
    cache.cache_points_data(42.3601, -71.0589, test_data)
    cache.cache_points_data(40.7128, -74.0060, test_data)  # NYC
    
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 2
    assert stats["valid_entries"] == 2
    print("✓ Stats with data test passed")
    
    print("Cache statistics tests passed!")

def test_cache_cleanup():
    """Test cache cleanup functionality."""
    print("\nTesting cache cleanup...")
    
    cache = NWSCache(cache_ttl=1)  # 1 second TTL
    
    # Add test data
    test_data = {"properties": {"test": "data"}}
    cache.cache_points_data(42.3601, -71.0589, test_data)
    cache.cache_points_data(40.7128, -74.0060, test_data)
    
    # Wait for expiration
    time.sleep(2)
    
    # Add fresh data
    cache.cache_points_data(34.0522, -118.2437, test_data)  # LA
    
    # Cleanup expired entries
    removed_count = cache.cleanup_expired()
    assert removed_count == 2, f"Should have removed 2 entries, got {removed_count}"
    
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 1, "Should have 1 entry remaining"
    print("✓ Cache cleanup test passed")
    
    print("Cache cleanup tests passed!")

def test_integration_with_weather_extractor():
    """Test cache integration with WeatherExtractor."""
    print("\nTesting cache integration with WeatherExtractor...")
    
    # Mock the config and other dependencies
    with patch('extractor.main.config') as mock_config, \
         patch('extractor.main.NWSConfig') as mock_nws_config, \
         patch('extractor.main.DatabaseManager') as mock_db:
        
        # Setup mocks
        mock_config.boston_lat = 42.3601
        mock_config.boston_lon = -71.0589
        mock_config.duckdb_path = "test.db"
        mock_nws_config.validate_coordinates.return_value = True
        
        # Import after mocking
        from main import WeatherExtractor
        
        # Create extractor
        extractor = WeatherExtractor()
        
        # Verify cache is initialized
        assert hasattr(extractor, 'nws_cache'), "WeatherExtractor should have nws_cache attribute"
        assert isinstance(extractor.nws_cache, NWSCache), "nws_cache should be NWSCache instance"
        assert extractor.nws_cache.cache_ttl == 3600, "Cache TTL should be 1 hour"
        
        print("✓ WeatherExtractor cache integration test passed")
    
    print("Integration tests passed!")

def main():
    """Run all tests."""
    print("Starting NWS cache integration tests...\n")
    
    try:
        test_nws_cache_basic_functionality()
        test_cache_stats()
        test_cache_cleanup()
        test_integration_with_weather_extractor()
        
        print("\n🎉 All NWS cache integration tests passed!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())