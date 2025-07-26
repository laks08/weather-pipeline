{ { config(materialized = 'table') } } with weather_summary as (
    select *
    from { { ref('int_weather_summary') } }
),
final as (
    select date,
        -- Temperature metrics
        round(final_avg_temp, 1) as avg_temperature_celsius,
        round(final_min_temp, 1) as min_temperature_celsius,
        round(final_max_temp, 1) as max_temperature_celsius,
        round(avg_feels_like, 1) as feels_like_temperature_celsius,
        round(day_night_temp_difference, 1) as day_night_temp_difference_celsius,
        -- Humidity and pressure
        round(avg_humidity, 0) as avg_humidity_percent,
        round(avg_pressure, 0) as avg_pressure_hpa,
        -- Wind metrics
        round(avg_wind_speed, 1) as avg_wind_speed_mps,
        round(avg_wind_speed * 2.237, 1) as avg_wind_speed_mph,
        -- Convert to mph
        -- Precipitation
        round(avg_precipitation_probability * 100, 1) as precipitation_probability_percent,
        round(forecast_precipitation_probability * 100, 1) as forecast_precipitation_probability_percent,
        -- Weather conditions
        most_common_description as primary_weather_condition,
        forecast_description as forecast_weather_condition,
        most_common_temp_category as temperature_category,
        most_common_wind_category as wind_category,
        forecast_temp_category as forecast_temperature_category,
        forecast_wind_category as forecast_wind_category,
        -- Seasonal information
        season,
        extract(
            month
            from date
        ) as month_number,
        extract(
            year
            from date
        ) as year,
        -- Data quality metrics
        readings_count as current_weather_readings,
        hourly_readings_count as hourly_forecast_readings,
        round(temp_variance_current_vs_hourly, 2) as temperature_variance_current_vs_forecast,
        -- Derived business metrics
        case
            when final_avg_temp < 0 then 'Freezing'
            when final_avg_temp < 10 then 'Cold'
            when final_avg_temp < 20 then 'Cool'
            when final_avg_temp < 30 then 'Warm'
            else 'Hot'
        end as business_temperature_category,
        case
            when avg_wind_speed_mps < 5 then 'Calm'
            when avg_wind_speed_mps < 15 then 'Moderate'
            when avg_wind_speed_mps < 25 then 'Strong'
            else 'Very Strong'
        end as business_wind_category,
        case
            when precipitation_probability_percent < 10 then 'Low'
            when precipitation_probability_percent < 50 then 'Medium'
            else 'High'
        end as business_precipitation_risk,
        -- Timestamps
        current_timestamp as last_updated_at
    from weather_summary
    where date is not null
)
select *
from final