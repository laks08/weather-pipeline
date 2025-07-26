#!/bin/bash

# Boston Weather Data ETL Pipeline Startup Script

echo "🌤️  Starting Boston Weather Data ETL Pipeline..."
echo "================================================"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    echo "Please copy env.example to .env:"
    echo "cp env.example .env"
    echo "The pipeline now uses the free NWS API (no API key required)"
    exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running!"
    echo "Please start Docker and try again."
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p data

echo "🚀 Starting services with Docker Compose..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "✅ Pipeline started successfully!"
echo ""
echo "🌐 Access points:"
echo "   - Dagster UI: http://localhost:3000"
echo "   - DuckDB data: ./data/weather.db"
echo ""
echo "📋 Useful commands:"
echo "   - View logs: docker-compose logs -f"
echo "   - Stop services: docker-compose down"
echo "   - Restart services: docker-compose restart"
echo ""
echo "🔍 Monitor the pipeline:"
echo "   - Open http://localhost:3000 in your browser"
echo "   - Check the Assets tab to see data flow"
echo "   - View scheduled runs in the Schedules tab" 