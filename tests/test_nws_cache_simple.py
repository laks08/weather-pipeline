#!/usr/bin/env python3
"""
Simple test script to verify NWS cache functionality.
"""
import sys
import os
import time

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from nws_cache import NWSCache

def test_cache_functionality():
    """Test the NWS cache functionality."""
    print("Testing NWS cache functionality...")
    
    # Create cache with 2 second TTL for testing
    cache = NWSCache(cache_ttl=2)
    
    # Test coordinates (Boston)
    lat, lon = 42.3601, -71.0589
    
    # Test data
    test_data = {
        "properties": {
            "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
            "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
            "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
        }
    }
    
    # Test 1: Cache miss
    result = cache.get_cached_points(lat, lon)
    assert result is None, "Should return None for cache miss"
    print("✓ Cache miss test passed")
    
    # Test 2: Cache data
    cache.cache_points_data(lat, lon, test_data)
    print("✓ Data cached successfully")
    
    # Test 3: Cache hit
    result = cache.get_cached_points(lat, lon)
    assert result is not None, "Should return cached data"
    assert result["properties"]["forecast"] == test_data["properties"]["forecast"]
    assert "_cached_at" not in result, "Should not return internal timestamp"
    print("✓ Cache hit test passed")
    
    # Test 4: Cache statistics
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 1
    assert stats["valid_entries"] == 1
    assert stats["cache_ttl_seconds"] == 2
    print("✓ Cache statistics test passed")
    
    # Test 5: Cache expiration
    print("Waiting for cache expiration...")
    time.sleep(3)
    result = cache.get_cached_points(lat, lon)
    assert result is None, "Should return None after expiration"
    print("✓ Cache expiration test passed")
    
    # Test 6: Cache cleanup
    # Create fresh cache for cleanup test
    cache = NWSCache(cache_ttl=1)  # 1 second TTL
    
    # Add multiple entries
    cache.cache_points_data(42.3601, -71.0589, test_data)  # Boston
    cache.cache_points_data(40.7128, -74.0060, test_data)  # NYC
    
    # Wait for expiration
    time.sleep(2)
    
    # Add fresh entry
    cache.cache_points_data(34.0522, -118.2437, test_data)  # LA
    
    # Cleanup should remove 2 expired entries
    removed_count = cache.cleanup_expired()
    assert removed_count == 2, f"Should remove 2 expired entries, got {removed_count}"
    
    # Verify only 1 entry remains
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 1, "Should have 1 entry remaining"
    print("✓ Cache cleanup test passed")
    
    # Test 7: Clear cache
    cache.clear_cache()
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 0, "Cache should be empty after clear"
    print("✓ Cache clear test passed")
    
    print("\n🎉 All cache functionality tests passed!")

if __name__ == "__main__":
    test_cache_functionality()