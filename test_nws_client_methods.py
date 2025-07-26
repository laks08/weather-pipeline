#!/usr/bin/env python3
"""
Test script for NWS API client methods.
"""
import sys
import os

# Add the extractor directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from config import NWSConfig, NWSAPIError, NWSGeographicError, NWSServiceUnavailableError
import nws_client

NWSAPIClient = nws_client.NWSAPIClient


def test_nws_client_methods():
    """Test NWS API client methods."""
    print("Testing NWS API Client Methods...")
    
    # Test with Boston coordinates
    lat, lon = 42.3601, -71.0589
    
    # Create client instance
    client = NWSAPIClient()
    
    try:
        # Test _get_nws_metadata method
        print("Testing _get_nws_metadata...")
        metadata = client._get_nws_metadata(lat, lon)
        assert isinstance(metadata, dict), "Metadata should be a dictionary"
        assert 'properties' in metadata, "Metadata should contain properties"
        print("✓ _get_nws_metadata method working")
        
        # Test _make_nws_request method
        print("Testing _make_nws_request...")
        points_url = NWSConfig.get_points_url(lat, lon)
        response = client._make_nws_request(points_url)
        assert isinstance(response, dict), "Response should be a dictionary"
        print("✓ _make_nws_request method working")
        
        # Test method signatures exist
        print("Testing method signatures...")
        assert hasattr(client, '_fetch_current_conditions'), "_fetch_current_conditions method should exist"
        assert hasattr(client, '_fetch_hourly_forecast'), "_fetch_hourly_forecast method should exist"
        assert hasattr(client, '_fetch_daily_forecast'), "_fetch_daily_forecast method should exist"
        print("✓ All required methods exist")
        
        print("🎉 NWS API client methods test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        client.close()


def test_error_handling():
    """Test error handling for invalid coordinates."""
    print("Testing error handling...")
    
    client = NWSAPIClient()
    
    try:
        # Test with coordinates outside NWS coverage (London)
        try:
            client._get_nws_metadata(51.5074, -0.1278)
            assert False, "Should have raised NWSGeographicError"
        except NWSGeographicError:
            print("✓ Geographic error handling working")
        
        print("🎉 Error handling test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False
    
    finally:
        client.close()


def main():
    """Run all tests."""
    print("Running NWS API Client Methods Tests\n")
    
    try:
        success1 = test_nws_client_methods()
        success2 = test_error_handling()
        
        if success1 and success2:
            print("\n🎉 All NWS API client methods tests passed!")
            return True
        else:
            print("\n❌ Some tests failed")
            return False
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)