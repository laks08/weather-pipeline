#!/usr/bin/env python3
"""
Test cache integration with the main weather extractor.
"""
import sys
import os
from unittest.mock import patch, MagicMock

# Add extractor directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

def test_cache_integration():
    """Test that the WeatherExtractor properly uses the cache."""
    print("Testing cache integration with WeatherExtractor...")
    
    # Mock all the dependencies to avoid database/file system issues
    with patch('extractor.main.config') as mock_config, \
         patch('extractor.main.NWSConfig') as mock_nws_config, \
         patch('extractor.main.os.makedirs'), \
         patch('extractor.main.DatabaseManager') as mock_db_manager, \
         patch('extractor.utils.duckdb.connect') as mock_duckdb:
        
        # Setup mocks
        mock_config.boston_lat = 42.3601
        mock_config.boston_lon = -71.0589
        mock_config.duckdb_path = "/tmp/test.db"
        mock_nws_config.validate_coordinates.return_value = True
        
        # Mock database manager context
        mock_db_instance = MagicMock()
        mock_db_manager.return_value.__enter__.return_value = mock_db_instance
        mock_db_manager.return_value.__exit__.return_value = None
        
        # Mock duckdb connection
        mock_connection = MagicMock()
        mock_duckdb.return_value = mock_connection
        
        # Import after mocking
        from main import WeatherExtractor
        from nws_cache import NWSCache
        
        # Create extractor
        extractor = WeatherExtractor()
        
        # Verify cache is properly initialized
        assert hasattr(extractor, 'nws_cache'), "WeatherExtractor should have nws_cache"
        assert isinstance(extractor.nws_cache, NWSCache), "nws_cache should be NWSCache instance"
        assert extractor.nws_cache.cache_ttl == 3600, "Cache TTL should be 1 hour (3600 seconds)"
        
        print("✓ Cache initialization test passed")
        
        # Test that _get_nws_metadata uses cache
        test_metadata = {
            "properties": {
                "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
                "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
                "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations"
            }
        }
        
        # Mock the API request and validation
        with patch.object(extractor, '_make_nws_request') as mock_request, \
             patch('extractor.main.validate_nws_response') as mock_validate:
            
            mock_request.return_value = test_metadata
            mock_validate.return_value = True
            
            # First call should hit the API and cache the result
            result1 = extractor._get_nws_metadata()
            assert result1 == test_metadata
            assert mock_request.call_count == 1, "Should make API request on first call"
            
            # Second call should use cache
            result2 = extractor._get_nws_metadata()
            assert result2 == test_metadata
            assert mock_request.call_count == 1, "Should not make additional API request (cache hit)"
            
            print("✓ Cache usage test passed")
        
        # Test cache statistics method
        stats = extractor.nws_cache.get_cache_stats()
        assert stats["total_entries"] == 1, "Should have 1 cached entry"
        assert stats["valid_entries"] == 1, "Should have 1 valid entry"
        
        print("✓ Cache statistics test passed")
        
        # Test cleanup method exists
        assert hasattr(extractor, 'cleanup_cache'), "Should have cleanup_cache method"
        extractor.cleanup_cache()  # Should not raise exception
        
        print("✓ Cache cleanup method test passed")
        
        print("\n🎉 All cache integration tests passed!")

if __name__ == "__main__":
    test_cache_integration()