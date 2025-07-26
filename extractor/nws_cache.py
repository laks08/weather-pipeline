"""
Caching mechanism for NWS API metadata to reduce redundant API calls.
"""
import time
from typing import Optional, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class NWSCache:
    """Cache NWS metadata and reduce API calls."""
    
    def __init__(self, cache_ttl: int = 3600):
        """
        Initialize the cache.
        
        Args:
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)
        """
        self.points_cache: Dict[Tuple[float, float], Dict[str, Any]] = {}
        self.cache_ttl = cache_ttl
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if a cache entry is expired."""
        return time.time() - timestamp > self.cache_ttl
    
    def get_cached_points(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get cached points data if available and not expired.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Cached points data if available and valid, None otherwise
        """
        cache_key = (lat, lon)
        
        if cache_key not in self.points_cache:
            logger.debug(f"No cached points data for coordinates ({lat}, {lon})")
            return None
        
        cached_entry = self.points_cache[cache_key]
        timestamp = cached_entry.get('_cached_at', 0)
        
        if self._is_expired(timestamp):
            logger.debug(f"Cached points data expired for coordinates ({lat}, {lon})")
            # Remove expired entry
            del self.points_cache[cache_key]
            return None
        
        logger.debug(f"Using cached points data for coordinates ({lat}, {lon})")
        # Return data without the internal timestamp
        data = cached_entry.copy()
        data.pop('_cached_at', None)
        return data
    
    def cache_points_data(self, lat: float, lon: float, data: Dict[str, Any]) -> None:
        """
        Cache points data with timestamp.
        
        Args:
            lat: Latitude
            lon: Longitude
            data: Points data to cache
        """
        cache_key = (lat, lon)
        
        # Add internal timestamp to the data
        cached_data = data.copy()
        cached_data['_cached_at'] = time.time()
        
        self.points_cache[cache_key] = cached_data
        logger.debug(f"Cached points data for coordinates ({lat}, {lon})")
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self.points_cache.clear()
        logger.info("NWS cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        total_entries = len(self.points_cache)
        expired_entries = 0
        
        current_time = time.time()
        for entry in self.points_cache.values():
            timestamp = entry.get('_cached_at', 0)
            if self._is_expired(timestamp):
                expired_entries += 1
        
        return {
            'total_entries': total_entries,
            'expired_entries': expired_entries,
            'valid_entries': total_entries - expired_entries,
            'cache_ttl_seconds': self.cache_ttl
        }
    
    def cleanup_expired(self) -> int:
        """
        Remove expired entries from cache.
        
        Returns:
            Number of entries removed
        """
        expired_keys = []
        
        for cache_key, cached_entry in self.points_cache.items():
            timestamp = cached_entry.get('_cached_at', 0)
            if self._is_expired(timestamp):
                expired_keys.append(cache_key)
        
        for key in expired_keys:
            del self.points_cache[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)