#!/usr/bin/env python3
"""
Test script to validate the weather ETL pipeline setup.
"""

import os
import sys
import subprocess
from pathlib import Path

def test_environment():
    """Test environment variables."""
    print("🔧 Testing environment variables...")
    
    required_vars = [
        'BOSTON_LAT', 
        'BOSTON_LON',
        'DUCKDB_PATH'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        return False
    else:
        print("✅ All environment variables are set")
        return True

def test_file_structure():
    """Test required files exist."""
    print("\n📁 Testing file structure...")
    
    required_files = [
        'extractor/main.py',
        'extractor/config.py', 
        'extractor/utils.py',
        'extractor/Dockerfile',
        'dbt/dbt_project.yml',
        'dbt/profiles.yml',
        'dbt/Dockerfile',
        'dagster/workspace.yaml',
        'dagster/Dockerfile',
        'weather_pipeline/__init__.py',
        'weather_pipeline/assets.py',
        'weather_pipeline/resources.py',
        'weather_pipeline/weather_pipeline.py',
        'docker-compose.yml'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files exist")
        return True

def test_python_imports():
    """Test Python imports."""
    print("\n🐍 Testing Python imports...")
    
    try:
        # Test extractor imports
        sys.path.append('extractor')
        import config
        import utils
        print("✅ Extractor imports successful")
        
        # Test weather_pipeline imports  
        from weather_pipeline import assets, resources, weather_pipeline
        print("✅ Weather pipeline imports successful")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_dbt_syntax():
    """Test DBT model syntax."""
    print("\n📊 Testing DBT model syntax...")
    
    dbt_models = [
        'dbt/models/staging/stg_current_weather.sql',
        'dbt/models/staging/stg_hourly_weather.sql', 
        'dbt/models/staging/stg_daily_weather.sql',
        'dbt/models/intermediate/int_weather_summary.sql',
        'dbt/models/marts/weather_summary.sql',
        'dbt/models/marts/weather_trends.sql'
    ]
    
    syntax_errors = []
    for model_path in dbt_models:
        if Path(model_path).exists():
            with open(model_path, 'r') as f:
                content = f.read()
                # Check for common syntax issues
                if '{ {' in content or '} }' in content:
                    syntax_errors.append(f"{model_path}: Invalid Jinja syntax")
        else:
            syntax_errors.append(f"{model_path}: File not found")
    
    if syntax_errors:
        print(f"❌ DBT syntax errors: {syntax_errors}")
        return False
    else:
        print("✅ DBT models syntax looks good")
        return True

def main():
    """Run all tests."""
    print("🧪 Running Weather ETL Pipeline Setup Tests")
    print("=" * 50)
    
    tests = [
        test_environment,
        test_file_structure, 
        test_python_imports,
        test_dbt_syntax
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    if all(results):
        print("🎉 All tests passed! The pipeline should be ready to run.")
        print("\nNext steps:")
        print("1. Start Docker: docker-compose up --build")
        print("2. Access Dagster UI: http://localhost:3000")
        return 0
    else:
        print("❌ Some tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())