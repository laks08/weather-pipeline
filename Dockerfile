FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Dagster and dependencies with compatible versions
RUN pip install --no-cache-dir \
    dagster==1.7.12 \
    dagster-duckdb==0.23.12 \
    dagster-dbt==0.23.12 \
    dagster-webserver==1.7.12 \
    duckdb==0.9.2 \
    requests==2.31.0 \
    structlog==23.2.0

# Copy project files
COPY . .

# Install weather_pipeline as editable package
RUN pip install -e .

# Create necessary directories
RUN mkdir -p /opt/dagster/dagster_home /data

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Expose Dagster UI port
EXPOSE 3000

# Start Dagster webserver with explicit workspace.yaml
CMD ["dagster", "dev", "-w", "/dagster/workspace.yaml", "-h", "0.0.0.0", "-p", "3000"] 