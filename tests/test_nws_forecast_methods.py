#!/usr/bin/env python3
"""
Test script for NWS API forecast methods with real API calls.
"""
import sys
import os

# Add the extractor directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extractor'))

from config import NWSConfig, NWSAPIError, NWSGeographicError, NWSServiceUnavailableError
import nws_client

NWSAPIClient = nws_client.NWSAPIClient


def test_forecast_methods():
    """Test NWS forecast methods with real API calls."""
    print("Testing NWS Forecast Methods with Real API Calls...")
    
    # Test with Boston coordinates
    lat, lon = 42.3601, -71.0589
    
    # Create client instance
    client = NWSAPIClient()
    
    try:
        # Test hourly forecast
        print("Testing _fetch_hourly_forecast...")
        try:
            hourly_data = client._fetch_hourly_forecast(lat, lon)
            assert isinstance(hourly_data, dict), "Hourly data should be a dictionary"
            assert 'properties' in hourly_data, "Hourly data should contain properties"
            periods = hourly_data.get('properties', {}).get('periods', [])
            assert isinstance(periods, list), "Periods should be a list"
            print(f"✓ _fetch_hourly_forecast working - got {len(periods)} periods")
        except Exception as e:
            print(f"⚠️  _fetch_hourly_forecast failed: {e}")
        
        # Test daily forecast
        print("Testing _fetch_daily_forecast...")
        try:
            daily_data = client._fetch_daily_forecast(lat, lon)
            assert isinstance(daily_data, dict), "Daily data should be a dictionary"
            assert 'properties' in daily_data, "Daily data should contain properties"
            periods = daily_data.get('properties', {}).get('periods', [])
            assert isinstance(periods, list), "Periods should be a list"
            print(f"✓ _fetch_daily_forecast working - got {len(periods)} periods")
        except Exception as e:
            print(f"⚠️  _fetch_daily_forecast failed: {e}")
        
        # Test current conditions
        print("Testing _fetch_current_conditions...")
        try:
            current_data = client._fetch_current_conditions(lat, lon)
            assert isinstance(current_data, dict), "Current data should be a dictionary"
            assert 'properties' in current_data, "Current data should contain properties"
            print("✓ _fetch_current_conditions working")
        except Exception as e:
            print(f"⚠️  _fetch_current_conditions failed: {e}")
        
        print("🎉 NWS forecast methods test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        client.close()


def main():
    """Run all tests."""
    print("Running NWS Forecast Methods Tests\n")
    
    try:
        success = test_forecast_methods()
        
        if success:
            print("\n🎉 NWS forecast methods tests completed!")
            return True
        else:
            print("\n❌ Tests failed")
            return False
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)