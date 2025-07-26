{ { config(materialized = 'table') } } with weather_summary as (
    select *
    from { { ref('int_weather_summary') } }
),
daily_trends as (
    select date,
        final_avg_temp,
        final_min_temp,
        final_max_temp,
        avg_humidity,
        avg_wind_speed,
        avg_precipitation_probability,
        season,
        -- Calculate moving averages
        avg(final_avg_temp) over (
            order by date rows between 6 preceding and current row
        ) as temp_7day_moving_avg,
        avg(final_avg_temp) over (
            order by date rows between 29 preceding and current row
        ) as temp_30day_moving_avg,
        avg(avg_humidity) over (
            order by date rows between 6 preceding and current row
        ) as humidity_7day_moving_avg,
        avg(avg_wind_speed) over (
            order by date rows between 6 preceding and current row
        ) as wind_7day_moving_avg,
        -- Calculate temperature ranges
        final_max_temp - final_min_temp as daily_temp_range,
        -- Calculate trend indicators
        case
            when final_avg_temp > lag(final_avg_temp, 1) over (
                order by date
            ) then 'increasing'
            when final_avg_temp < lag(final_avg_temp, 1) over (
                order by date
            ) then 'decreasing'
            else 'stable'
        end as temp_trend_direction,
        -- Calculate volatility
        stddev(final_avg_temp) over (
            order by date rows between 6 preceding and current row
        ) as temp_7day_volatility
    from weather_summary
    where final_avg_temp is not null
),
seasonal_analysis as (
    select date,
        final_avg_temp,
        season,
        -- Seasonal averages
        avg(final_avg_temp) over (partition by season) as seasonal_avg_temp,
        -- Deviation from seasonal average
        final_avg_temp - avg(final_avg_temp) over (partition by season) as temp_deviation_from_seasonal_avg,
        -- Seasonal ranking
        rank() over (
            partition by season
            order by final_avg_temp desc
        ) as temp_rank_in_season
    from weather_summary
    where final_avg_temp is not null
),
weather_patterns as (
    select date,
        final_avg_temp,
        avg_humidity,
        avg_wind_speed,
        avg_precipitation_probability,
        -- Weather pattern classification
        case
            when final_avg_temp < 0
            and avg_precipitation_probability > 0.5 then 'cold_wet'
            when final_avg_temp < 0
            and avg_precipitation_probability <= 0.5 then 'cold_dry'
            when final_avg_temp between 0 and 15
            and avg_precipitation_probability > 0.5 then 'cool_wet'
            when final_avg_temp between 0 and 15
            and avg_precipitation_probability <= 0.5 then 'cool_dry'
            when final_avg_temp between 15 and 25
            and avg_precipitation_probability > 0.5 then 'mild_wet'
            when final_avg_temp between 15 and 25
            and avg_precipitation_probability <= 0.5 then 'mild_dry'
            when final_avg_temp > 25
            and avg_precipitation_probability > 0.5 then 'warm_wet'
            when final_avg_temp > 25
            and avg_precipitation_probability <= 0.5 then 'warm_dry'
            else 'unknown'
        end as weather_pattern,
        -- Extreme weather indicators
        case
            when final_avg_temp > 30 then 'heat_wave'
            when final_avg_temp < -10 then 'cold_snap'
            when avg_wind_speed > 20 then 'high_winds'
            when avg_precipitation_probability > 0.8 then 'heavy_precipitation_expected'
            else 'normal'
        end as extreme_weather_indicator
    from weather_summary
    where final_avg_temp is not null
)
select w.date,
    -- Basic metrics
    round(w.final_avg_temp, 1) as avg_temperature_celsius,
    round(w.final_min_temp, 1) as min_temperature_celsius,
    round(w.final_max_temp, 1) as max_temperature_celsius,
    round(w.avg_humidity, 0) as avg_humidity_percent,
    round(w.avg_wind_speed, 1) as avg_wind_speed_mps,
    round(w.avg_precipitation_probability * 100, 1) as precipitation_probability_percent,
    -- Trend metrics
    round(dt.temp_7day_moving_avg, 1) as temperature_7day_moving_avg,
    round(dt.temp_30day_moving_avg, 1) as temperature_30day_moving_avg,
    round(dt.humidity_7day_moving_avg, 0) as humidity_7day_moving_avg,
    round(dt.wind_7day_moving_avg, 1) as wind_7day_moving_avg,
    round(dt.daily_temp_range, 1) as daily_temperature_range,
    dt.temp_trend_direction,
    round(dt.temp_7day_volatility, 2) as temperature_7day_volatility,
    -- Seasonal analysis
    w.season,
    round(sa.seasonal_avg_temp, 1) as seasonal_average_temperature,
    round(sa.temp_deviation_from_seasonal_avg, 1) as temperature_deviation_from_seasonal_avg,
    sa.temp_rank_in_season,
    -- Weather patterns
    wp.weather_pattern,
    wp.extreme_weather_indicator,
    -- Derived insights
    case
        when sa.temp_deviation_from_seasonal_avg > 5 then 'unusually_warm'
        when sa.temp_deviation_from_seasonal_avg < -5 then 'unusually_cold'
        else 'seasonal_normal'
    end as temperature_anomaly,
    case
        when dt.temp_7day_volatility > 10 then 'high_variability'
        when dt.temp_7day_volatility > 5 then 'moderate_variability'
        else 'low_variability'
    end as temperature_stability,
    -- Timestamps
    current_timestamp as last_updated_at
from weather_summary w
    left join daily_trends dt on w.date = dt.date
    left join seasonal_analysis sa on w.date = sa.date
    left join weather_patterns wp on w.date = wp.date
where w.date is not null