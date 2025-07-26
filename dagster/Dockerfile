FROM python:3.9-slim

WORKDIR /dagster

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY extractor/requirements.txt /tmp/extractor-requirements.txt

# Install Dagster and dependencies
RUN pip install \
    dagster==1.5.12 \
    dagster-webserver==1.5.12 \
    dagster-duckdb==0.21.12 \
    dagster-dbt==0.21.12

# Install extractor dependencies
RUN pip install -r /tmp/extractor-requirements.txt

# Copy all code
COPY . .

# Create directories
RUN mkdir -p /data
RUN mkdir -p /opt/dagster/dagster_home

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Expose port
EXPOSE 3000

# Start Dagster dev server
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "weather_pipeline"]