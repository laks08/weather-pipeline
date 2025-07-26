# Boston Weather Data ETL Pipeline (4D Stack)

A comprehensive weather data pipeline that fetches Boston weather data from the National Weather Service (NWS) API, processes it with DuckDB, transforms it with DBT, and orchestrates everything with Dagster - all containerized with Docker.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│ National        │───▶│ Python       │───▶│ DuckDB      │───▶│ DBT         │
│ Weather Service │    │ Extractor    │    │ Warehouse   │    │ Transform   │
│ API (Free)      │    │ + NWS Cache  │    │             │    │             │
└─────────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
                                │                   │
                                ▼                   ▼
                       ┌──────────────┐    ┌─────────────┐
                       │ Dagster      │    │ Docker      │
                       │ Orchestrator │    │ Containers  │
                       └──────────────┘    └─────────────┘
```

### NWS API Workflow

The pipeline uses a multi-step NWS API workflow:

1. **Points API**: `/points/{lat},{lon}` → Get forecast URLs and station info
2. **Current Conditions**: `/stations/{id}/observations/latest` → Real-time weather
3. **Hourly Forecast**: `/gridpoints/{office}/{x},{y}/forecast/hourly` → 156-hour forecast
4. **Daily Forecast**: `/gridpoints/{office}/{x},{y}/forecast` → 7-day forecast

## 🚀 Features

- **Real-time Data Collection**: Fetches current, hourly, and daily weather data for Boston
- **Efficient Storage**: Uses DuckDB for fast analytical queries
- **Data Transformation**: DBT models for cleaning and aggregating weather data
- **Scheduled Orchestration**: Dagster manages data pipeline scheduling
- **Containerized**: Fully dockerized for easy deployment
- **Monitoring**: Comprehensive logging and error handling

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.9+

**Note**: No API key required - uses the free National Weather Service API.

## 🛠️ Setup

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd weather-etl
   ```

2. **Set up environment variables**

   ```bash
   cp .env.example .env
   # No API key required - uses free NWS API
   ```

3. **Start the pipeline**

   ```bash
   docker-compose up -d
   ```

4. **Access services**
   - Dagster UI: <http://localhost:3000>
   - DuckDB data: Located in `./data/weather.db`

## 📊 Data Flow

### 1. Data Extraction

- **Current Weather**: Every 10 minutes
- **Hourly Forecast**: Every 10 minutes
- **Daily Forecast**: Once per day
- **Location**: Boston (lat=42.3601, lon=-71.0589)

### 2. Data Storage

- `current_weather`: Real-time conditions
- `hourly_weather`: 48-hour forecast
- `daily_weather`: 7-day forecast

### 3. Data Transformation

- Weather data cleaning and validation
- Aggregated weather statistics
- Time-based analysis and trends

## 🗂️ Project Structure

```
weather-etl/
├── docker-compose.yml          # Main orchestration
├── .env.example               # Environment template
├── README.md                  # This file
├── requirements.txt           # Python dependencies
├── extractor/
│   ├── Dockerfile            # Python extractor container
│   ├── main.py              # Main extraction logic
│   ├── config.py            # Configuration management
│   └── utils.py             # Utility functions
├── dbt/
│   ├── Dockerfile           # DBT container
│   ├── dbt_project.yml      # DBT project config
│   ├── profiles.yml         # DBT profiles
│   └── models/              # DBT transformation models
├── dagster/
│   ├── Dockerfile           # Dagster container
│   ├── workspace.yaml       # Dagster workspace
│   └── weather_pipeline/    # Dagster pipeline code
├── data/                    # DuckDB database files
└── notebooks/               # Jupyter notebooks for analysis
```

## 🔧 Configuration

### Environment Variables

- `BOSTON_LAT`: Boston latitude (default: 42.3601)
- `BOSTON_LON`: Boston longitude (default: -71.0589)
- `DAGSTER_HOME`: Dagster home directory

**Note**: No API key required as the pipeline uses the free NWS API.

### NWS API Details

The pipeline uses the free National Weather Service API with these characteristics:

- **Base URL**: `https://api.weather.gov`
- **No rate limits**: Unlimited requests (with reasonable usage)
- **No API key required**: No registration or authentication needed
- **User-Agent required**: Must include contact information in headers
- **Geographic coverage**: United States and territories only
- **Current weather**: Fetched from nearest weather station
- **Forecasts**: Up to 7 days daily, 156 hours hourly
- **Caching**: Points metadata cached for 1 hour to optimize API calls

## 📈 Usage

### Starting the Pipeline

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Accessing Data

```bash
# Connect to DuckDB
docker exec -it weather-etl-duckdb-1 duckdb data/weather.db

# Query weather data
SELECT * FROM current_weather ORDER BY timestamp DESC LIMIT 5;
```

### Dagster UI

- Open <http://localhost:3000>
- Monitor pipeline runs
- View asset materializations
- Check logs and errors

## 🔍 Data Models

### Raw Tables

- `current_weather`: Current conditions
- `hourly_weather`: Hourly forecasts
- `daily_weather`: Daily forecasts

### Transformed Models

- `weather_summary`: Daily weather summaries
- `weather_trends`: Temperature and precipitation trends
- `weather_alerts`: Severe weather conditions

## 🐛 Troubleshooting

### Common Issues

1. **NWS API Connection Issues**

   - Check network connectivity to `api.weather.gov`
   - Verify User-Agent header is properly set
   - Check if coordinates are within U.S. coverage area

2. **Geographic Coverage Issues**

   - NWS API only covers U.S. and territories
   - Boston coordinates (42.3601, -71.0589) are supported
   - Verify location is within NWS grid coverage

3. **Docker Issues**

   - Check if Docker and Docker Compose are running
   - Ensure ports 3000 and 8080 are available
   - Verify container health status

4. **Data Quality Issues**
   - Some weather stations may not report all parameters
   - NWS API may have temporary service windows
   - Check logs for transformation errors

### Logs

```bash
# View extractor logs
docker-compose logs extractor

# View Dagster logs
docker-compose logs dagster

# View DBT logs
docker-compose logs dbt
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- National Weather Service for providing the free weather API
- DuckDB for fast analytical database
- DBT for data transformation
- Dagster for pipeline orchestration
