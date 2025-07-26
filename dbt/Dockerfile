FROM python:3.9-slim

WORKDIR /dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-duckdb
RUN pip install dbt-duckdb==1.6.2

# Copy dbt project files
COPY . .

# Create data directory
RUN mkdir -p /data

# Set environment variables
ENV DBT_PROFILES_DIR=/dbt

# Default command (can be overridden)
CMD ["tail", "-f", "/dev/null"]