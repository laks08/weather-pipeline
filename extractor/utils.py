"""
Utility functions for the Boston Weather ETL Pipeline.
"""
import duckdb
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import structlog
from config import DatabaseConfig
import re

logger = structlog.get_logger()


class DatabaseManager:
    """Manages DuckDB database operations."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
    
    def __enter__(self):
        self.connection = duckdb.connect(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
    
    def initialize_database(self):
        """Initialize database tables and indexes."""
        try:
            # Create tables
            self.connection.execute(DatabaseConfig.CURRENT_WEATHER_SCHEMA)
            self.connection.execute(DatabaseConfig.HOURLY_WEATHER_SCHEMA)
            self.connection.execute(DatabaseConfig.DAILY_WEATHER_SCHEMA)
            
            # Create indexes
            for index_sql in DatabaseConfig.INDEXES:
                self.connection.execute(index_sql)
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def insert_current_weather(self, data: Dict[str, Any]):
        """Insert current weather data into the database."""
        try:
            df = pd.DataFrame([data])
            self.connection.execute(
                "INSERT INTO current_weather VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                df.values.tolist()[0]
            )
            logger.info("Current weather data inserted successfully")
            
        except Exception as e:
            logger.error(f"Failed to insert current weather data: {e}")
            raise
    
    def insert_hourly_weather(self, data_list: List[Dict[str, Any]]):
        """Insert hourly weather data into the database."""
        try:
            if not data_list:
                logger.warning("No hourly weather data to insert")
                return
            
            df = pd.DataFrame(data_list)
            self.connection.executemany(
                "INSERT INTO hourly_weather VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                df.values.tolist()
            )
            logger.info(f"Inserted {len(data_list)} hourly weather records")
            
        except Exception as e:
            logger.error(f"Failed to insert hourly weather data: {e}")
            raise
    
    def insert_daily_weather(self, data_list: List[Dict[str, Any]]):
        """Insert daily weather data into the database."""
        try:
            if not data_list:
                logger.warning("No daily weather data to insert")
                return
            
            df = pd.DataFrame(data_list)
            self.connection.executemany(
                "INSERT INTO daily_weather VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                df.values.tolist()
            )
            logger.info(f"Inserted {len(data_list)} daily weather records")
            
        except Exception as e:
            logger.error(f"Failed to insert daily weather data: {e}")
            raise
    
    def get_latest_current_weather(self) -> Optional[Dict[str, Any]]:
        """Get the most recent current weather record."""
        try:
            result = self.connection.execute(
                "SELECT * FROM current_weather ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            
            if result:
                columns = [desc[0] for desc in self.connection.description]
                return dict(zip(columns, result))
            return None
            
        except Exception as e:
            logger.error(f"Failed to get latest current weather: {e}")
            return None
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dictionaries."""
        try:
            result = self.connection.execute(query).fetchall()
            
            if result:
                # Get column names from the cursor description
                columns = [desc[0] for desc in self.connection.description]
                return [dict(zip(columns, row)) for row in result]
            return []
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return []


def parse_timestamp(timestamp: int) -> datetime:
    """Convert Unix timestamp to datetime object."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def extract_weather_data(api_response: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Extract and transform weather data from API response."""
    try:
        data = {
            'current': [],
            'hourly': [],
            'daily': []
        }
        
        # Extract current weather
        if 'current' in api_response:
            current = api_response['current']
            current_data = {
                'timestamp': parse_timestamp(current['dt']),
                'temp': current['temp'],
                'feels_like': current['feels_like'],
                'humidity': current['humidity'],
                'pressure': current['pressure'],
                'wind_speed': current['wind_speed'],
                'wind_deg': current['wind_deg'],
                'description': current['weather'][0]['description'],
                'icon': current['weather'][0]['icon']
            }
            data['current'].append(current_data)
        
        # Extract hourly weather (next 48 hours)
        if 'hourly' in api_response:
            for hour_data in api_response['hourly'][:48]:  # Limit to 48 hours
                hourly_data = {
                    'timestamp': parse_timestamp(hour_data['dt']),
                    'temp': hour_data['temp'],
                    'feels_like': hour_data['feels_like'],
                    'humidity': hour_data['humidity'],
                    'pressure': hour_data['pressure'],
                    'wind_speed': hour_data['wind_speed'],
                    'wind_deg': hour_data['wind_deg'],
                    'description': hour_data['weather'][0]['description'],
                    'icon': hour_data['weather'][0]['icon'],
                    'pop': hour_data.get('pop', 0.0)
                }
                data['hourly'].append(hourly_data)
        
        # Extract daily weather (next 7 days)
        if 'daily' in api_response:
            for day_data in api_response['daily'][:7]:  # Limit to 7 days
                daily_data = {
                    'date': parse_timestamp(day_data['dt']).date(),
                    'temp_min': day_data['temp']['min'],
                    'temp_max': day_data['temp']['max'],
                    'temp_day': day_data['temp']['day'],
                    'temp_night': day_data['temp']['night'],
                    'humidity': day_data['humidity'],
                    'pressure': day_data['pressure'],
                    'wind_speed': day_data['wind_speed'],
                    'wind_deg': day_data['wind_deg'],
                    'description': day_data['weather'][0]['description'],
                    'icon': day_data['weather'][0]['icon'],
                    'pop': day_data.get('pop', 0.0)
                }
                data['daily'].append(daily_data)
        
        logger.info(f"Extracted weather data: {len(data['current'])} current, "
                   f"{len(data['hourly'])} hourly, {len(data['daily'])} daily records")
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to extract weather data: {e}")
        raise


def validate_api_response(response: Dict[str, Any]) -> bool:
    """Validate the API response structure."""
    required_fields = ['current', 'hourly', 'daily']
    
    for field in required_fields:
        if field not in response:
            logger.error(f"Missing required field in API response: {field}")
            return False
    
    # Validate current weather structure
    if 'current' in response:
        current = response['current']
        required_current_fields = ['dt', 'temp', 'feels_like', 'humidity', 
                                 'pressure', 'wind_speed', 'wind_deg', 'weather']
        
        for field in required_current_fields:
            if field not in current:
                logger.error(f"Missing required field in current weather: {field}")
                return False
    
    logger.info("API response validation successful")
    return True


def format_weather_description(weather_data: Dict[str, Any]) -> str:
    """Format weather data into a human-readable description."""
    try:
        temp = weather_data.get('temp', 'N/A')
        description = weather_data.get('description', 'Unknown')
        humidity = weather_data.get('humidity', 'N/A')
        wind_speed = weather_data.get('wind_speed', 'N/A')
        
        return f"{temp}°C, {description}, Humidity: {humidity}%, Wind: {wind_speed} m/s"
        
    except Exception as e:
        logger.error(f"Failed to format weather description: {e}")
        return "Weather data unavailable"


def calculate_api_usage_stats() -> Dict[str, Any]:
    """Calculate API usage statistics."""
    try:
        with DatabaseManager("/data/weather.db") as db:
            # Count records by type
            current_count = db.connection.execute(
                "SELECT COUNT(*) FROM current_weather"
            ).fetchone()[0]
            
            hourly_count = db.connection.execute(
                "SELECT COUNT(*) FROM hourly_weather"
            ).fetchone()[0]
            
            daily_count = db.connection.execute(
                "SELECT COUNT(*) FROM daily_weather"
            ).fetchone()[0]
            
            # Get latest update time
            latest_update = db.connection.execute(
                "SELECT MAX(timestamp) FROM current_weather"
            ).fetchone()[0]
            
            return {
                'current_records': current_count,
                'hourly_records': hourly_count,
                'daily_records': daily_count,
                'latest_update': latest_update,
                'estimated_daily_requests': current_count + hourly_count + daily_count
            }
            
    except Exception as e:
        logger.error(f"Failed to calculate API usage stats: {e}")
        return {}


# NWS Data Transformation Functions

def validate_nws_response(response: Dict[str, Any], response_type: str) -> bool:
    """
    Validate NWS API response structure.
    
    Args:
        response: The NWS API response dictionary
        response_type: Type of response ('current', 'hourly', 'daily', 'points')
        
    Returns:
        bool: True if response is valid, False otherwise
    """
    try:
        if not isinstance(response, dict):
            logger.error(f"NWS {response_type} response is not a dictionary")
            return False
        
        if response_type == 'points':
            # Validate points response structure
            if 'properties' not in response:
                logger.error("NWS points response missing 'properties' field")
                return False
            
            properties = response['properties']
            required_fields = ['forecast', 'forecastHourly']
            for field in required_fields:
                if field not in properties:
                    logger.error(f"NWS points response missing required field: {field}")
                    return False
        
        elif response_type == 'current':
            # Validate current conditions response structure
            if 'properties' not in response:
                logger.error("NWS current conditions response missing 'properties' field")
                return False
            
            properties = response['properties']
            # Temperature is the most critical field for current conditions
            if 'temperature' not in properties:
                logger.error("NWS current conditions response missing temperature")
                return False
        
        elif response_type in ['hourly', 'daily']:
            # Validate forecast response structure
            if 'properties' not in response:
                logger.error(f"NWS {response_type} forecast response missing 'properties' field")
                return False
            
            properties = response['properties']
            if 'periods' not in properties:
                logger.error(f"NWS {response_type} forecast response missing 'periods' field")
                return False
            
            if not isinstance(properties['periods'], list):
                logger.error(f"NWS {response_type} forecast 'periods' is not a list")
                return False
        
        logger.debug(f"NWS {response_type} response validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Failed to validate NWS {response_type} response: {e}")
        return False


def _convert_temperature(temp_value: Optional[float], unit_code: str = "wmoUnit:degC") -> Optional[float]:
    """
    Convert temperature to Celsius if needed.
    
    Args:
        temp_value: Temperature value
        unit_code: Unit code from NWS API
        
    Returns:
        Temperature in Celsius or None if conversion fails
    """
    if temp_value is None:
        return None
    
    try:
        if unit_code == "wmoUnit:degC":
            return float(temp_value)
        elif unit_code == "wmoUnit:degF":
            # Convert Fahrenheit to Celsius
            return (float(temp_value) - 32) * 5.0 / 9.0
        elif unit_code == "wmoUnit:K":
            # Convert Kelvin to Celsius
            return float(temp_value) - 273.15
        else:
            logger.warning(f"Unknown temperature unit: {unit_code}, assuming Celsius")
            return float(temp_value)
    except (ValueError, TypeError):
        logger.error(f"Failed to convert temperature: {temp_value} {unit_code}")
        return None


def _convert_pressure(pressure_value: Optional[float], unit_code: str = "wmoUnit:Pa") -> Optional[int]:
    """
    Convert pressure to hPa (hectopascals).
    
    Args:
        pressure_value: Pressure value
        unit_code: Unit code from NWS API
        
    Returns:
        Pressure in hPa as integer or None if conversion fails
    """
    if pressure_value is None:
        return None
    
    try:
        if unit_code == "wmoUnit:Pa":
            # Convert Pascals to hectopascals (hPa)
            return int(float(pressure_value) / 100)
        elif unit_code == "wmoUnit:hPa":
            return int(float(pressure_value))
        else:
            logger.warning(f"Unknown pressure unit: {unit_code}, assuming Pascals")
            return int(float(pressure_value) / 100)
    except (ValueError, TypeError):
        logger.error(f"Failed to convert pressure: {pressure_value} {unit_code}")
        return None


def _convert_wind_speed(wind_value: Optional[float], unit_code: str = "wmoUnit:m_s-1") -> Optional[float]:
    """
    Convert wind speed to m/s.
    
    Args:
        wind_value: Wind speed value
        unit_code: Unit code from NWS API
        
    Returns:
        Wind speed in m/s or None if conversion fails
    """
    if wind_value is None:
        return None
    
    try:
        if unit_code == "wmoUnit:m_s-1":
            return float(wind_value)
        elif unit_code == "wmoUnit:km_h-1":
            # Convert km/h to m/s
            return float(wind_value) / 3.6
        elif unit_code == "wmoUnit:mi_h-1":
            # Convert mph to m/s
            return float(wind_value) * 0.44704
        else:
            logger.warning(f"Unknown wind speed unit: {unit_code}, assuming m/s")
            return float(wind_value)
    except (ValueError, TypeError):
        logger.error(f"Failed to convert wind speed: {wind_value} {unit_code}")
        return None


def _map_nws_icon_to_weather_icon(description: str) -> str:
    """
    Map NWS weather description to standard weather icon codes.
    
    Args:
        description: NWS weather description text
        
    Returns:
        Standard weather icon code (compatible with existing schema)
    """
    if not description:
        return "01d"  # Default clear sky
    
    description_lower = description.lower()
    
    # Map common weather conditions to icon codes
    if "clear" in description_lower or "sunny" in description_lower:
        return "01d"
    elif "few clouds" in description_lower or "partly cloudy" in description_lower:
        return "02d"
    elif "scattered clouds" in description_lower:
        return "03d"
    elif "broken clouds" in description_lower or "overcast" in description_lower:
        return "04d"
    elif "shower" in description_lower or "light rain" in description_lower:
        return "09d"
    elif "rain" in description_lower:
        return "10d"
    elif "thunderstorm" in description_lower:
        return "11d"
    elif "snow" in description_lower:
        return "13d"
    elif "mist" in description_lower or "fog" in description_lower:
        return "50d"
    else:
        return "01d"  # Default to clear sky


def transform_nws_current_weather(nws_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform NWS current conditions to match existing schema.
    
    Args:
        nws_data: NWS current conditions API response
        
    Returns:
        Dict matching existing current weather schema or None if transformation fails
    """
    try:
        if not validate_nws_response(nws_data, 'current'):
            return None
        
        properties = nws_data.get('properties', {})
        
        # Extract temperature data
        temp_data = properties.get('temperature', {})
        temp_value = temp_data.get('value') if isinstance(temp_data, dict) else None
        temp_unit = temp_data.get('unitCode', 'wmoUnit:degC') if isinstance(temp_data, dict) else 'wmoUnit:degC'
        temp = _convert_temperature(temp_value, temp_unit)
        
        # Extract feels-like temperature (use heat index or wind chill)
        feels_like = None
        heat_index_data = properties.get('heatIndex', {})
        wind_chill_data = properties.get('windChill', {})
        
        if isinstance(heat_index_data, dict) and heat_index_data.get('value') is not None:
            feels_like = _convert_temperature(
                heat_index_data.get('value'),
                heat_index_data.get('unitCode', 'wmoUnit:degC')
            )
        elif isinstance(wind_chill_data, dict) and wind_chill_data.get('value') is not None:
            feels_like = _convert_temperature(
                wind_chill_data.get('value'),
                wind_chill_data.get('unitCode', 'wmoUnit:degC')
            )
        else:
            # Fallback to regular temperature if no feels-like data
            feels_like = temp
        
        # Extract humidity
        humidity_data = properties.get('relativeHumidity', {})
        humidity = None
        if isinstance(humidity_data, dict) and humidity_data.get('value') is not None:
            try:
                humidity = int(float(humidity_data['value']))
            except (ValueError, TypeError):
                humidity = None
        
        # Extract pressure
        pressure_data = properties.get('barometricPressure', {})
        pressure = None
        if isinstance(pressure_data, dict) and pressure_data.get('value') is not None:
            pressure = _convert_pressure(
                pressure_data.get('value'),
                pressure_data.get('unitCode', 'wmoUnit:Pa')
            )
        
        # Extract wind data
        wind_speed_data = properties.get('windSpeed', {})
        wind_speed = None
        if isinstance(wind_speed_data, dict) and wind_speed_data.get('value') is not None:
            wind_speed = _convert_wind_speed(
                wind_speed_data.get('value'),
                wind_speed_data.get('unitCode', 'wmoUnit:m_s-1')
            )
        
        wind_direction_data = properties.get('windDirection', {})
        wind_deg = None
        if isinstance(wind_direction_data, dict) and wind_direction_data.get('value') is not None:
            try:
                wind_deg = int(float(wind_direction_data['value']))
            except (ValueError, TypeError):
                wind_deg = None
        
        # Extract description
        description = properties.get('textDescription', 'Unknown')
        
        # Generate icon code
        icon = _map_nws_icon_to_weather_icon(description)
        
        # Parse timestamp
        timestamp_str = properties.get('timestamp')
        timestamp = None
        if timestamp_str:
            try:
                from dateutil.parser import parse
                timestamp = parse(timestamp_str)
            except ImportError:
                # Fallback to basic ISO format parsing if dateutil not available
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except Exception:
                    timestamp = datetime.now(timezone.utc)
            except Exception:
                timestamp = datetime.now(timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)
        
        transformed_data = {
            'timestamp': timestamp,
            'temp': temp,
            'feels_like': feels_like,
            'humidity': humidity,
            'pressure': pressure,
            'wind_speed': wind_speed,
            'wind_deg': wind_deg,
            'description': description,
            'icon': icon
        }
        
        logger.debug("Successfully transformed NWS current weather data")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Failed to transform NWS current weather data: {e}")
        return None


def transform_nws_hourly_forecast(nws_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform NWS hourly forecast to match existing schema.
    
    Args:
        nws_data: NWS hourly forecast API response
        
    Returns:
        List of dicts matching existing hourly weather schema
    """
    try:
        if not validate_nws_response(nws_data, 'hourly'):
            return []
        
        properties = nws_data.get('properties', {})
        periods = properties.get('periods', [])
        
        transformed_data = []
        
        # Limit to 48 hours to match existing schema
        for period in periods[:48]:
            try:
                # Parse timestamp
                start_time_str = period.get('startTime')
                timestamp = None
                if start_time_str:
                    try:
                        from dateutil.parser import parse
                        timestamp = parse(start_time_str)
                    except ImportError:
                        # Fallback to basic ISO format parsing if dateutil not available
                        try:
                            timestamp = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        except Exception:
                            continue  # Skip this period if timestamp parsing fails
                    except Exception:
                        continue  # Skip this period if timestamp parsing fails
                else:
                    continue  # Skip if no timestamp
                
                # Extract temperature
                temp = period.get('temperature')
                temp_unit = period.get('temperatureUnit', 'F')
                
                # Convert temperature to Celsius
                if temp is not None:
                    if temp_unit == 'F':
                        temp = (float(temp) - 32) * 5.0 / 9.0
                    else:
                        temp = float(temp)
                
                # NWS hourly forecast doesn't provide feels_like, use temperature
                feels_like = temp
                
                # Extract humidity (may not be available in all periods)
                humidity = period.get('relativeHumidity', {})
                if isinstance(humidity, dict):
                    humidity = humidity.get('value')
                if humidity is not None:
                    try:
                        humidity = int(float(humidity))
                    except (ValueError, TypeError):
                        humidity = None
                
                # Extract wind data
                wind_speed_str = period.get('windSpeed', '0 mph')
                wind_speed = None
                if wind_speed_str:
                    try:
                        # Parse wind speed (e.g., "10 mph" or "5 to 10 mph")
                        import re
                        wind_match = re.search(r'(\d+(?:\.\d+)?)', wind_speed_str)
                        if wind_match:
                            wind_mph = float(wind_match.group(1))
                            wind_speed = wind_mph * 0.44704  # Convert mph to m/s
                    except Exception:
                        wind_speed = None
                
                wind_direction = period.get('windDirection', 'N')
                wind_deg = None
                if wind_direction:
                    # Convert wind direction to degrees
                    direction_map = {
                        'N': 0, 'NNE': 22, 'NE': 45, 'ENE': 67,
                        'E': 90, 'ESE': 112, 'SE': 135, 'SSE': 157,
                        'S': 180, 'SSW': 202, 'SW': 225, 'WSW': 247,
                        'W': 270, 'WNW': 292, 'NW': 315, 'NNW': 337
                    }
                    wind_deg = direction_map.get(wind_direction, 0)
                
                # Extract description and icon
                description = period.get('shortForecast', 'Unknown')
                icon = _map_nws_icon_to_weather_icon(description)
                
                # Extract probability of precipitation
                pop = period.get('probabilityOfPrecipitation', {})
                if isinstance(pop, dict):
                    pop = pop.get('value', 0)
                if pop is not None:
                    try:
                        pop = float(pop) / 100.0  # Convert percentage to decimal
                    except (ValueError, TypeError):
                        pop = 0.0
                else:
                    pop = 0.0
                
                hourly_data = {
                    'timestamp': timestamp,
                    'temp': temp,
                    'feels_like': feels_like,
                    'humidity': humidity,
                    'pressure': None,  # Not available in NWS hourly forecast
                    'wind_speed': wind_speed,
                    'wind_deg': wind_deg,
                    'description': description,
                    'icon': icon,
                    'pop': pop
                }
                
                transformed_data.append(hourly_data)
                
            except Exception as e:
                logger.warning(f"Failed to transform hourly period: {e}")
                continue  # Skip this period and continue with others
        
        logger.info(f"Successfully transformed {len(transformed_data)} hourly forecast periods")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Failed to transform NWS hourly forecast data: {e}")
        return []


def transform_nws_daily_forecast(nws_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform NWS daily forecast to match existing schema.
    
    Args:
        nws_data: NWS daily forecast API response
        
    Returns:
        List of dicts matching existing daily weather schema
    """
    try:
        if not validate_nws_response(nws_data, 'daily'):
            return []
        
        properties = nws_data.get('properties', {})
        periods = properties.get('periods', [])
        
        transformed_data = []
        daily_data_map = {}  # Group day and night periods
        
        # Process periods and group by date
        for period in periods:
            try:
                # Parse timestamp
                start_time_str = period.get('startTime')
                if not start_time_str:
                    continue
                
                try:
                    from dateutil.parser import parse
                    start_time = parse(start_time_str)
                    date = start_time.date()
                except ImportError:
                    # Fallback to basic ISO format parsing if dateutil not available
                    try:
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        date = start_time.date()
                    except Exception:
                        continue
                except Exception:
                    continue
                
                # Initialize daily data if not exists
                if date not in daily_data_map:
                    daily_data_map[date] = {
                        'date': date,
                        'temp_min': None,
                        'temp_max': None,
                        'temp_day': None,
                        'temp_night': None,
                        'humidity': None,
                        'pressure': None,
                        'wind_speed': None,
                        'wind_deg': None,
                        'description': None,
                        'icon': None,
                        'pop': 0.0
                    }
                
                # Extract temperature
                temp = period.get('temperature')
                temp_unit = period.get('temperatureUnit', 'F')
                
                if temp is not None:
                    # Convert to Celsius
                    if temp_unit == 'F':
                        temp_celsius = (float(temp) - 32) * 5.0 / 9.0
                    else:
                        temp_celsius = float(temp)
                    
                    # Determine if this is day or night period
                    is_daytime = period.get('isDaytime', True)
                    
                    if is_daytime:
                        daily_data_map[date]['temp_day'] = temp_celsius
                        daily_data_map[date]['temp_max'] = temp_celsius
                    else:
                        daily_data_map[date]['temp_night'] = temp_celsius
                        daily_data_map[date]['temp_min'] = temp_celsius
                
                # Extract wind data (use daytime values preferentially)
                if period.get('isDaytime', True) or daily_data_map[date]['wind_speed'] is None:
                    wind_speed_str = period.get('windSpeed', '0 mph')
                    if wind_speed_str:
                        try:
                            import re
                            wind_match = re.search(r'(\d+(?:\.\d+)?)', wind_speed_str)
                            if wind_match:
                                wind_mph = float(wind_match.group(1))
                                daily_data_map[date]['wind_speed'] = wind_mph * 0.44704  # Convert to m/s
                        except Exception:
                            pass
                    
                    wind_direction = period.get('windDirection', 'N')
                    if wind_direction:
                        direction_map = {
                            'N': 0, 'NNE': 22, 'NE': 45, 'ENE': 67,
                            'E': 90, 'ESE': 112, 'SE': 135, 'SSE': 157,
                            'S': 180, 'SSW': 202, 'SW': 225, 'WSW': 247,
                            'W': 270, 'WNW': 292, 'NW': 315, 'NNW': 337
                        }
                        daily_data_map[date]['wind_deg'] = direction_map.get(wind_direction, 0)
                
                # Extract description and icon (use daytime values preferentially)
                if period.get('isDaytime', True) or daily_data_map[date]['description'] is None:
                    description = period.get('shortForecast', 'Unknown')
                    daily_data_map[date]['description'] = description
                    daily_data_map[date]['icon'] = _map_nws_icon_to_weather_icon(description)
                
                # Extract probability of precipitation (use maximum)
                pop = period.get('probabilityOfPrecipitation', {})
                if isinstance(pop, dict):
                    pop = pop.get('value', 0)
                if pop is not None:
                    try:
                        pop_decimal = float(pop) / 100.0
                        daily_data_map[date]['pop'] = max(daily_data_map[date]['pop'], pop_decimal)
                    except (ValueError, TypeError):
                        pass
                
            except Exception as e:
                logger.warning(f"Failed to process daily period: {e}")
                continue
        
        # Convert to list and limit to 7 days
        sorted_dates = sorted(daily_data_map.keys())[:7]
        for date in sorted_dates:
            daily_data = daily_data_map[date]
            
            # Ensure we have min/max temperatures
            if daily_data['temp_min'] is None and daily_data['temp_max'] is not None:
                daily_data['temp_min'] = daily_data['temp_max'] - 5  # Estimate
            elif daily_data['temp_max'] is None and daily_data['temp_min'] is not None:
                daily_data['temp_max'] = daily_data['temp_min'] + 5  # Estimate
            
            # Ensure we have day/night temperatures
            if daily_data['temp_day'] is None:
                daily_data['temp_day'] = daily_data['temp_max']
            if daily_data['temp_night'] is None:
                daily_data['temp_night'] = daily_data['temp_min']
            
            transformed_data.append(daily_data)
        
        logger.info(f"Successfully transformed {len(transformed_data)} daily forecast periods")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Failed to transform NWS daily forecast data: {e}")
        return [] 