# Boston Weather Data ETL Pipeline - Setup Guide

## 🚀 Quick Start

1. **Clone and navigate to the project:**

   ```bash
   cd weather-etl
   ```

2. **Set up environment variables:**

   ```bash
   cp env.example .env
   # No API key required - uses free NWS API
   ```

3. **Start the pipeline:**

   ```bash
   ./start.sh
   ```

4. **Access the services:**
   - Dagster UI: <http://localhost:3000>
   - DuckDB data: `./data/weather.db`

## 📋 Prerequisites

- **Docker & Docker Compose**: Required for running the containerized services
- **Python 3.9+**: For local development (optional)

**Note**: This pipeline now uses the free National Weather Service (NWS) API, which requires no API key or registration.

## 🏗️ Architecture Overview

The pipeline consists of four main components:

### 1. **Python Extractor** (`extractor/`)

- Fetches weather data from National Weather Service (NWS) API
- Stores data in DuckDB tables
- Runs on a schedule (every 10 minutes for current/hourly, daily for forecasts)

### 2. **DuckDB Database**

- Fast analytical database for weather data storage
- Stores raw data in three tables:
  - `current_weather`: Real-time conditions
  - `hourly_weather`: 48-hour forecasts
  - `daily_weather`: 7-day forecasts

### 3. **DBT Transformations** (`dbt/`)

- Cleans and validates raw weather data
- Creates staging, intermediate, and mart models
- Generates business-ready analytics tables

### 4. **Dagster Orchestration** (`dagster/`)

- Manages the entire data pipeline
- Schedules data extraction and transformation
- Provides monitoring and observability

## 📊 Data Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────┐
│ NWS Points API  │───▶│ Python Extractor │───▶│ DuckDB      │
│ (Metadata)      │    │ + Caching        │    │ Storage     │
└─────────────────┘    └──────────────────┘    └─────────────┘
                                │                       │
┌─────────────────┐             │                       ▼
│ NWS Forecast    │─────────────┘              ┌─────────────┐
│ APIs (3 types)  │                           │ DBT         │
└─────────────────┘                           │ Transform   │
                                              └─────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────┐
                                              │ Analytics   │
                                              │ Tables      │
                                              └─────────────┘
                                                       ▲
                                              ┌─────────────┐
                                              │ Dagster     │
                                              │ Orchestrate │
                                              └─────────────┘
```

### Detailed NWS API Workflow

1. **Metadata Fetch**: Get forecast URLs from `/points/{lat},{lon}`
2. **Data Extraction**: Fetch current, hourly, and daily data from specific URLs
3. **Data Transformation**: Convert NWS format to standardized schema
4. **Storage**: Insert transformed data into DuckDB tables
5. **Analytics**: DBT processes raw data into business-ready models

## 🔧 Configuration

### Environment Variables (`.env`)

```bash
# Location coordinates (defaults shown)
BOSTON_LAT=42.3601
BOSTON_LON=-71.0589

# Optional configuration
LOG_LEVEL=INFO
```

**Note**: No API key is required as the pipeline uses the free NWS API.

### NWS API Requirements

- **User-Agent Header**: Required for all requests (automatically handled by the pipeline)
- **Geographic Limitations**: Only works for U.S. locations (Boston is supported)
- **No Authentication**: No API key or registration required
- **Rate Limiting**: No hard limits, but reasonable usage expected
- **Service Availability**: Government API may have maintenance windows

### NWS API Configuration

The pipeline uses the free National Weather Service API with these specifications:

- **Base URL**: `https://api.weather.gov`
- **No rate limits**: Unlimited requests (with reasonable usage)
- **No API key required**: No registration or authentication needed
- **User-Agent required**: Must include contact information in headers
- **Geographic coverage**: United States and territories only
- **Current weather**: Fetched from nearest weather station
- **Forecasts**: Up to 7 days daily, 156 hours hourly
- **Caching**: Points metadata cached for 1 hour to optimize API calls

#### NWS API Endpoints Used

1. **Points API**: `/points/{lat},{lon}` - Get forecast URLs and grid info
2. **Current Conditions**: `/stations/{id}/observations/latest` - Real-time data
3. **Hourly Forecast**: `/gridpoints/{office}/{x},{y}/forecast/hourly` - 156-hour forecast
4. **Daily Forecast**: `/gridpoints/{office}/{x},{y}/forecast` - 7-day forecast

## 📈 Data Models

### Raw Tables

- `current_weather`: Current conditions with timestamps
- `hourly_weather`: Hourly forecasts with precipitation probability
- `daily_weather`: Daily forecasts with min/max temperatures

### Transformed Models (DBT)

- `stg_*`: Staging models with data cleaning
- `int_weather_summary`: Intermediate aggregated data
- `weather_summary`: Business-ready daily summaries
- `weather_trends`: Time-series analysis and trends

### NWS Data Characteristics

- **Temperature**: Provided in Celsius (converted from Fahrenheit if needed)
- **Pressure**: Converted from Pascals to hectoPascals (hPa)
- **Wind Speed**: Provided in meters per second
- **Timestamps**: UTC timestamps converted to local time
- **Missing Data**: Some weather stations may not report all parameters
- **Update Frequency**: Current conditions updated every 10 minutes, forecasts less frequently

## 🐳 Docker Services

The pipeline runs in four containers:

1. **duckdb**: Database service
2. **extractor**: Python weather data extractor
3. **dbt**: Data transformation service
4. **dagster**: Orchestration and monitoring

## 📝 Usage Examples

### View Current Weather Data

```sql
-- Connect to DuckDB
docker exec -it weather-etl-duckdb-1 duckdb data/weather.db

-- Query latest weather
SELECT * FROM current_weather ORDER BY timestamp DESC LIMIT 5;
```

### Run DBT Transformations

```bash
# Execute DBT models
docker exec -it weather-etl-dbt-1 dbt run

# Run tests
docker exec -it weather-etl-dbt-1 dbt test

# Generate documentation
docker exec -it weather-etl-dbt-1 dbt docs generate
```

### Monitor with Dagster

1. Open <http://localhost:3000>
2. Navigate to Assets tab to see data lineage
3. Check Schedules tab for automated runs
4. View logs and error handling

## 🔍 Troubleshooting

### Common Issues

1. **NWS API Connection Issues**

   ```bash
   # Check NWS API connectivity
   docker-compose logs extractor | grep "NWS"

   # Test NWS API directly
   curl -H "User-Agent: boston-weather-etl (your-email@example.com)" \
        "https://api.weather.gov/points/42.3601,-71.0589"
   ```

2. **Geographic Coverage Issues**

   ```bash
   # Verify coordinates are within NWS coverage
   curl -H "User-Agent: boston-weather-etl (your-email@example.com)" \
        "https://api.weather.gov/points/42.3601,-71.0589" | jq '.properties'
   ```

3. **Database Connection Issues**

   ```bash
   # Check DuckDB service
   docker-compose logs duckdb

   # Test database connection
   docker exec -it weather-etl-duckdb-1 duckdb data/weather.db -c "SELECT 1;"
   ```

4. **DBT Transformation Failures**

   ```bash
   # Check DBT logs
   docker-compose logs dbt

   # Run DBT with debug info
   docker exec -it weather-etl-dbt-1 dbt run --debug
   ```

5. **NWS API Rate Limiting or Service Issues**

   ```bash
   # Check for NWS service status
   curl -I "https://api.weather.gov/points/42.3601,-71.0589"

   # Check extractor retry logic
   docker-compose logs extractor | grep -i "retry\|error"
   ```

### Logs and Monitoring

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f extractor
docker-compose logs -f dagster
docker-compose logs -f dbt
```

## 🧪 Testing

### Manual Testing

```bash
# Test NWS API connection with proper headers
curl -H "User-Agent: boston-weather-etl (your-email@example.com)" \
     "https://api.weather.gov/points/42.3601,-71.0589"

# Test forecast endpoints
curl -H "User-Agent: boston-weather-etl (your-email@example.com)" \
     "https://api.weather.gov/gridpoints/BOX/71,90/forecast"

# Test database connection
docker exec -it weather-etl-duckdb-1 duckdb data/weather.db -c "SELECT COUNT(*) FROM current_weather;"

# Verify data transformation
docker exec -it weather-etl-duckdb-1 duckdb data/weather.db -c "
  SELECT timestamp, temp, humidity, description
  FROM current_weather
  ORDER BY timestamp DESC
  LIMIT 3;"
```

### Automated Testing

```bash
# Run DBT tests
docker exec -it weather-etl-dbt-1 dbt test

# Run Python tests (if implemented)
docker exec -it weather-etl-extractor-1 python -m pytest
```

## 📚 Additional Resources

- **National Weather Service API**: [Documentation](https://www.weather.gov/documentation/services-web-api)
- **DuckDB**: [Documentation](https://duckdb.org/docs/)
- **DBT**: [Documentation](https://docs.getdbt.com/)
- **Dagster**: [Documentation](https://docs.dagster.io/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

---

## Happy Weather Data Processing! 🌤️
