"""
Configuration management for the Boston Weather ETL Pipeline.
"""
import os
from typing import Optional, Dict, Any
try:
    from pydantic_settings import BaseSettings
    from pydantic import validator
except ImportError:
    from pydantic import BaseSettings, validator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class WeatherConfig(BaseSettings):
    """Configuration settings for the weather ETL pipeline."""
    
    # Location Configuration
    boston_lat: float = 42.3601
    boston_lon: float = -71.0589
    
    # Database Configuration
    duckdb_path: str = "/data/weather.db"
    
    # Logging Configuration
    log_level: str = "INFO"
    
    # Scheduling Configuration
    current_weather_interval: int = 10  # minutes
    hourly_weather_interval: int = 10   # minutes
    daily_weather_interval: int = 1440  # minutes (24 hours)
    
    @validator('boston_lat')
    def validate_lat(cls, v):
        if not -90 <= v <= 90:
            raise ValueError("Latitude must be between -90 and 90")
        return v
    
    @validator('boston_lon')
    def validate_lon(cls, v):
        if not -180 <= v <= 180:
            raise ValueError("Longitude must be between -180 and 180")
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra environment variables


# Global configuration instance
config = WeatherConfig()





class DatabaseConfig:
    """Database configuration and table schemas."""
    
    # Table schemas
    CURRENT_WEATHER_SCHEMA = """
        CREATE TABLE IF NOT EXISTS current_weather (
            timestamp TIMESTAMP,
            temp REAL,
            feels_like REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            wind_deg INTEGER,
            description TEXT,
            icon TEXT
        )
    """
    
    HOURLY_WEATHER_SCHEMA = """
        CREATE TABLE IF NOT EXISTS hourly_weather (
            timestamp TIMESTAMP,
            temp REAL,
            feels_like REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            wind_deg INTEGER,
            description TEXT,
            icon TEXT,
            pop REAL
        )
    """
    
    DAILY_WEATHER_SCHEMA = """
        CREATE TABLE IF NOT EXISTS daily_weather (
            date DATE,
            temp_min REAL,
            temp_max REAL,
            temp_day REAL,
            temp_night REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            wind_deg INTEGER,
            description TEXT,
            icon TEXT,
            pop REAL
        )
    """
    
    # Indexes for better query performance
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_current_weather_timestamp ON current_weather(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_hourly_weather_timestamp ON hourly_weather(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_daily_weather_date ON daily_weather(date)",
    ]


# NWS API Error Classes
class NWSAPIError(Exception):
    """Base exception for NWS API errors."""
    pass


class NWSGeographicError(NWSAPIError):
    """Raised when coordinates are outside NWS coverage."""
    pass


class NWSServiceUnavailableError(NWSAPIError):
    """Raised when NWS API is temporarily unavailable."""
    pass


class NWSConfig:
    """National Weather Service API configuration."""
    
    BASE_URL = "https://api.weather.gov"
    USER_AGENT = "boston-weather-etl (contact@example.com)"
    TIMEOUT = 30
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1  # seconds
    
    @classmethod
    def get_points_url(cls, lat: float, lon: float) -> str:
        """Generate the Points API URL for getting forecast URLs."""
        return f"{cls.BASE_URL}/points/{lat},{lon}"
    
    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Get required headers for NWS API requests."""
        return {
            "User-Agent": cls.USER_AGENT,
            "Accept": "application/json"
        }
    
    @classmethod
    def validate_coordinates(cls, lat: float, lon: float) -> bool:
        """Validate that coordinates are within NWS coverage area (US territories)."""
        # Basic validation for continental US, Alaska, Hawaii, and territories
        # Continental US: roughly 24.5°N to 49.4°N, -125°W to -66.9°W
        # Alaska: roughly 51.2°N to 71.4°N, -179.1°W to -129.9°W
        # Hawaii: roughly 18.9°N to 28.4°N, -178.3°W to -154.8°W
        
        # Continental US
        if 24.5 <= lat <= 49.4 and -125.0 <= lon <= -66.9:
            return True
        # Alaska
        if 51.2 <= lat <= 71.4 and -179.1 <= lon <= -129.9:
            return True
        # Hawaii
        if 18.9 <= lat <= 28.4 and -178.3 <= lon <= -154.8:
            return True
        # Puerto Rico and other territories
        if 17.8 <= lat <= 18.6 and -67.3 <= lon <= -65.2:
            return True
        
        return False 