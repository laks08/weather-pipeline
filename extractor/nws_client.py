"""
National Weather Service API client with retry logic and error handling.
"""
import time
import requests
from typing import Optional, Dict, Any
from functools import wraps
import logging

from config import NWSConfig, NWSAPIError, NWSGeographicError, NWSServiceUnavailableError
from nws_cache import NWSCache

logger = logging.getLogger(__name__)


def retry_with_exponential_backoff(max_attempts: int = 3, base_delay: float = 1.0):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds (will be exponentially increased)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except NWSServiceUnavailableError as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"NWS API unavailable, retrying in {delay}s (attempt {attempt + 1}/{max_attempts})")
                        time.sleep(delay)
                    else:
                        logger.error(f"NWS API unavailable after {max_attempts} attempts")
                        raise
                except (NWSGeographicError, NWSAPIError) as e:
                    # Don't retry for geographic errors or other API errors
                    logger.error(f"NWS API error (no retry): {e}")
                    raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Request failed, retrying in {delay}s (attempt {attempt + 1}/{max_attempts}): {e}")
                        time.sleep(delay)
                    else:
                        logger.error(f"Request failed after {max_attempts} attempts: {e}")
                        raise
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def handle_nws_error(response: requests.Response) -> None:
    """
    Handle NWS API error responses with appropriate exceptions.
    
    Args:
        response: HTTP response object
        
    Raises:
        NWSGeographicError: When coordinates are outside NWS coverage
        NWSServiceUnavailableError: When NWS API is temporarily unavailable
        NWSAPIError: For other API-related errors
    """
    if response.status_code == 404:
        raise NWSGeographicError("Location outside NWS coverage area")
    elif response.status_code == 503:
        raise NWSServiceUnavailableError("NWS API temporarily unavailable")
    elif response.status_code == 500:
        raise NWSServiceUnavailableError("NWS API internal server error")
    elif response.status_code == 429:
        raise NWSServiceUnavailableError("NWS API rate limit exceeded")
    elif not response.ok:
        raise NWSAPIError(f"NWS API error: {response.status_code} - {response.text}")


class NWSAPIClient:
    """Client for making requests to the National Weather Service API."""
    
    def __init__(self, cache_ttl: int = 3600):
        self.config = NWSConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.get_headers())
        self.cache = NWSCache(cache_ttl=cache_ttl)
    
    @retry_with_exponential_backoff(max_attempts=NWSConfig.RETRY_ATTEMPTS, base_delay=NWSConfig.RETRY_DELAY)
    def make_request(self, url: str) -> Dict[str, Any]:
        """
        Make a request to the NWS API with proper error handling and retry logic.
        
        Args:
            url: The API endpoint URL
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            NWSAPIError: For API-related errors
            NWSGeographicError: When location is outside NWS coverage
            NWSServiceUnavailableError: When API is temporarily unavailable
        """
        try:
            logger.debug(f"Making NWS API request to: {url}")
            
            response = self.session.get(url, timeout=self.config.TIMEOUT)
            
            # Handle error responses
            if not response.ok:
                handle_nws_error(response)
            
            data = response.json()
            logger.debug(f"NWS API request successful: {url}")
            return data
            
        except requests.exceptions.Timeout:
            raise NWSServiceUnavailableError("NWS API request timed out")
        except requests.exceptions.ConnectionError:
            raise NWSServiceUnavailableError("Failed to connect to NWS API")
        except requests.exceptions.RequestException as e:
            raise NWSAPIError(f"NWS API request failed: {e}")
        except ValueError as e:
            # JSON decode error
            raise NWSAPIError(f"Invalid JSON response from NWS API: {e}")
    
    def get_points_metadata(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Get NWS point metadata including forecast URLs.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict containing points metadata with forecast URLs
        """
        # Validate coordinates are within NWS coverage
        if not self.config.validate_coordinates(lat, lon):
            raise NWSGeographicError(f"Coordinates ({lat}, {lon}) are outside NWS coverage area")
        
        url = self.config.get_points_url(lat, lon)
        return self.make_request(url)
    
    def _get_nws_metadata(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch NWS points data and forecast URLs for given coordinates.
        Uses caching to avoid redundant API calls.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict containing NWS metadata with forecast URLs
            
        Raises:
            NWSGeographicError: When coordinates are outside NWS coverage
            NWSAPIError: For other API-related errors
        """
        # Check cache first
        cached_data = self.cache.get_cached_points(lat, lon)
        if cached_data is not None:
            logger.debug(f"Using cached metadata for coordinates ({lat}, {lon})")
            return cached_data
        
        # Fetch from API if not cached
        metadata = self.get_points_metadata(lat, lon)
        
        # Cache the result
        self.cache.cache_points_data(lat, lon, metadata)
        
        return metadata
    
    def _make_nws_request(self, url: str) -> Dict[str, Any]:
        """
        Make a request to NWS API with proper headers and error handling.
        
        Args:
            url: The NWS API endpoint URL
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            NWSAPIError: For API-related errors
            NWSGeographicError: When location is outside NWS coverage
            NWSServiceUnavailableError: When API is temporarily unavailable
        """
        return self.make_request(url)
    
    def _fetch_current_conditions(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch current weather conditions from NWS API.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict containing current weather observations
            
        Raises:
            NWSAPIError: For API-related errors
        """
        # First get the metadata to find the observation station
        metadata = self._get_nws_metadata(lat, lon)
        
        # Extract the observation stations URL from metadata
        properties = metadata.get('properties', {})
        observation_stations_url = properties.get('observationStations')
        
        if not observation_stations_url:
            raise NWSAPIError("No observation stations URL found in metadata")
        
        # Get the list of observation stations
        stations_data = self._make_nws_request(observation_stations_url)
        stations = stations_data.get('features', [])
        
        if not stations:
            raise NWSAPIError("No observation stations found for location")
        
        # Try to get current observations from the first available station
        for station in stations:
            try:
                station_id = station.get('properties', {}).get('stationIdentifier')
                if station_id:
                    observations_url = f"{self.config.BASE_URL}/stations/{station_id}/observations/latest"
                    return self._make_nws_request(observations_url)
            except (NWSAPIError, NWSServiceUnavailableError):
                # Try next station if this one fails
                continue
        
        raise NWSAPIError("Unable to fetch current conditions from any observation station")
    
    def _fetch_hourly_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch hourly forecast data from NWS API.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict containing hourly forecast data
            
        Raises:
            NWSAPIError: For API-related errors
        """
        # Get metadata to find forecast URLs
        metadata = self._get_nws_metadata(lat, lon)
        
        # Extract the hourly forecast URL
        properties = metadata.get('properties', {})
        forecast_hourly_url = properties.get('forecastHourly')
        
        if not forecast_hourly_url:
            raise NWSAPIError("No hourly forecast URL found in metadata")
        
        return self._make_nws_request(forecast_hourly_url)
    
    def _fetch_daily_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Fetch daily forecast data from NWS API.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict containing daily forecast data
            
        Raises:
            NWSAPIError: For API-related errors
        """
        # Get metadata to find forecast URLs
        metadata = self._get_nws_metadata(lat, lon)
        
        # Extract the daily forecast URL
        properties = metadata.get('properties', {})
        forecast_url = properties.get('forecast')
        
        if not forecast_url:
            raise NWSAPIError("No daily forecast URL found in metadata")
        
        return self._make_nws_request(forecast_url)
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()