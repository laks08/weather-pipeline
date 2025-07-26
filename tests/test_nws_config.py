#!/usr/bin/env python3
"""
Test script for NWS API configuration and client infrastructure.
"""
import sys
import os

# Add the extractor directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

# Import the modules directly
import config
from config import NWSConfig, NWSAPIError, NWSGeographicError, NWSServiceUnavailableError

# Fix the relative imports by importing the modules directly
import nws_client
import nws_cache

# Get the classes we need
NWSAPIClient = nws_client.NWSAPIClient
NWSCache = nws_cache.NWSCache


def test_nws_config():
    """Test NWS configuration class."""
    print("Testing NWS Configuration...")
    
    # Test URL generation
    url = NWSConfig.get_points_url(42.3601, -71.0589)
    expected_url = "https://api.weather.gov/points/42.3601,-71.0589"
    assert url == expected_url, f"Expected {expected_url}, got {url}"
    print(f"✓ Points URL generation: {url}")
    
    # Test headers
    headers = NWSConfig.get_headers()
    assert "User-Agent" in headers, "User-Agent header missing"
    assert "Accept" in headers, "Accept header missing"
    assert headers["Accept"] == "application/json", "Accept header incorrect"
    print(f"✓ Headers generation: {headers}")
    
    # Test coordinate validation
    # Boston (should be valid)
    assert NWSConfig.validate_coordinates(42.3601, -71.0589), "Boston coordinates should be valid"
    print("✓ Boston coordinates validation passed")
    
    # London (should be invalid)
    assert not NWSConfig.validate_coordinates(51.5074, -0.1278), "London coordinates should be invalid"
    print("✓ London coordinates validation passed")
    
    print("NWS Configuration tests passed!\n")


def test_nws_cache():
    """Test NWS cache functionality."""
    print("Testing NWS Cache...")
    
    cache = NWSCache(cache_ttl=2)  # 2 second TTL for testing
    
    # Test empty cache
    result = cache.get_cached_points(42.3601, -71.0589)
    assert result is None, "Empty cache should return None"
    print("✓ Empty cache test passed")
    
    # Test caching data
    test_data = {"properties": {"forecast": "test_url"}}
    cache.cache_points_data(42.3601, -71.0589, test_data)
    
    # Test retrieving cached data
    cached_result = cache.get_cached_points(42.3601, -71.0589)
    assert cached_result is not None, "Cached data should be available"
    assert cached_result["properties"]["forecast"] == "test_url", "Cached data incorrect"
    print("✓ Cache storage and retrieval test passed")
    
    # Test cache stats
    stats = cache.get_cache_stats()
    assert stats["total_entries"] == 1, "Should have 1 cache entry"
    assert stats["valid_entries"] == 1, "Should have 1 valid entry"
    print(f"✓ Cache stats: {stats}")
    
    # Test cache expiration (wait for TTL)
    import time
    time.sleep(3)  # Wait for cache to expire
    
    expired_result = cache.get_cached_points(42.3601, -71.0589)
    assert expired_result is None, "Expired cache should return None"
    print("✓ Cache expiration test passed")
    
    print("NWS Cache tests passed!\n")


def test_error_classes():
    """Test NWS error classes."""
    print("Testing NWS Error Classes...")
    
    # Test base error
    try:
        raise NWSAPIError("Test error")
    except NWSAPIError as e:
        assert str(e) == "Test error", "Base error message incorrect"
        print("✓ NWSAPIError test passed")
    
    # Test geographic error
    try:
        raise NWSGeographicError("Location outside coverage")
    except NWSGeographicError as e:
        assert isinstance(e, NWSAPIError), "Geographic error should inherit from NWSAPIError"
        print("✓ NWSGeographicError test passed")
    
    # Test service unavailable error
    try:
        raise NWSServiceUnavailableError("Service down")
    except NWSServiceUnavailableError as e:
        assert isinstance(e, NWSAPIError), "Service error should inherit from NWSAPIError"
        print("✓ NWSServiceUnavailableError test passed")
    
    print("NWS Error Classes tests passed!\n")


def main():
    """Run all tests."""
    print("Running NWS API Configuration and Client Infrastructure Tests\n")
    
    try:
        test_nws_config()
        test_nws_cache()
        test_error_classes()
        
        print("🎉 All tests passed! NWS API configuration and client infrastructure is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)