#!/usr/bin/env python3
"""
Comprehensive test for NWS cache functionality and integration.
"""
import sys
import os
import time
from unittest.mock import patch, MagicMock

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from nws_cache import NWSCache
from nws_client import NWSAPIClient

def test_cache_with_nws_client():
    """Test cache functionality with NWS API client."""
    print("Testing NWS cache with API client...")
    
    # Mock response data
    mock_points_response = {
        "properties": {
            "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
            "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
            "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
        }
    }
    
    # Create client with short cache TTL for testing
    client = NWSAPIClient(cache_ttl=2)
    
    # Test coordinates
    lat, lon = 42.3601, -71.0589
    
    # Mock the actual HTTP request
    with patch.object(client, 'make_request') as mock_request:
        mock_request.return_value = mock_points_response
        
        # First call should hit the API
        result1 = client._get_nws_metadata(lat, lon)
        assert result1 == mock_points_response
        assert mock_request.call_count == 1, "Should make API request on first call"
        
        # Second call should use cache
        result2 = client._get_nws_metadata(lat, lon)
        assert result2 == mock_points_response
        assert mock_request.call_count == 1, "Should not make additional API request (cache hit)"
        
        print("✓ API client cache usage test passed")
        
        # Wait for cache expiration
        print("Waiting for cache expiration...")
        time.sleep(3)
        
        # Third call should hit API again after expiration
        result3 = client._get_nws_metadata(lat, lon)
        assert result3 == mock_points_response
        assert mock_request.call_count == 2, "Should make API request after cache expiration"
        
        print("✓ Cache expiration with API client test passed")
    
    client.close()
    print("NWS cache with API client tests passed!")

def test_cache_performance():
    """Test cache performance benefits."""
    print("\nTesting cache performance benefits...")
    
    cache = NWSCache(cache_ttl=3600)
    
    # Simulate API response time
    def mock_slow_api_call():
        time.sleep(0.1)  # Simulate 100ms API call
        return {"properties": {"test": "data"}}
    
    lat, lon = 42.3601, -71.0589
    
    # Time first call (cache miss)
    start_time = time.time()
    test_data = mock_slow_api_call()
    cache.cache_points_data(lat, lon, test_data)
    first_call_time = time.time() - start_time
    
    # Time second call (cache hit)
    start_time = time.time()
    cached_result = cache.get_cached_points(lat, lon)
    second_call_time = time.time() - start_time
    
    assert cached_result is not None, "Should get cached data"
    assert second_call_time < first_call_time, "Cache hit should be faster than API call"
    
    print(f"✓ First call (API): {first_call_time:.3f}s")
    print(f"✓ Second call (cache): {second_call_time:.3f}s")
    print(f"✓ Performance improvement: {first_call_time/second_call_time:.1f}x faster")
    
    print("Cache performance test passed!")

def test_cache_memory_usage():
    """Test cache memory usage and limits."""
    print("\nTesting cache memory usage...")
    
    cache = NWSCache(cache_ttl=3600)
    
    # Add multiple entries
    test_data = {"properties": {"test": "data"}}
    coordinates = [
        (42.3601, -71.0589),  # Boston
        (40.7128, -74.0060),  # NYC
        (34.0522, -118.2437), # LA
        (41.8781, -87.6298),  # Chicago
        (29.7604, -95.3698),  # Houston
    ]
    
    for lat, lon in coordinates:
        cache.cache_points_data(lat, lon, test_data)
    
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 5, "Should have 5 cached entries"
    assert stats["valid_entries"] == 5, "All entries should be valid"
    
    # Test cache clear
    cache.clear_cache()
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 0, "Cache should be empty after clear"
    
    print("✓ Cache memory usage test passed")

def main():
    """Run all comprehensive tests."""
    print("Starting comprehensive NWS cache tests...\n")
    
    try:
        test_cache_with_nws_client()
        test_cache_performance()
        test_cache_memory_usage()
        
        print("\n🎉 All comprehensive NWS cache tests passed!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())