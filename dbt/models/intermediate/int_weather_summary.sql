{ { config(materialized = 'view') } } with current_weather as (
    select *
    from { { ref('stg_current_weather') } }
),
hourly_weather as (
    select *
    from { { ref('stg_hourly_weather') } }
),
daily_weather as (
    select *
    from { { ref('stg_daily_weather') } }
),
-- Daily summary from current weather data
current_daily_summary as (
    select date(timestamp) as date,
        avg(temp) as avg_temp,
        min(temp) as min_temp,
        max(temp) as max_temp,
        avg(feels_like) as avg_feels_like,
        avg(humidity) as avg_humidity,
        avg(pressure) as avg_pressure,
        avg(wind_speed) as avg_wind_speed,
        count(*) as readings_count,
        -- Most common weather conditions
        mode(description) as most_common_description,
        mode(temp_category) as most_common_temp_category,
        mode(wind_category) as most_common_wind_category
    from current_weather
    group by date(timestamp)
),
-- Hourly summary
hourly_daily_summary as (
    select date(timestamp) as date,
        avg(temp) as avg_temp_hourly,
        min(temp) as min_temp_hourly,
        max(temp) as max_temp_hourly,
        avg(humidity) as avg_humidity_hourly,
        avg(wind_speed) as avg_wind_speed_hourly,
        avg(pop) as avg_precipitation_probability,
        count(*) as hourly_readings_count,
        -- Peak hours analysis
        avg(
            case
                when hour_of_day between 6 and 18 then temp
            end
        ) as avg_daytime_temp,
        avg(
            case
                when hour_of_day < 6
                or hour_of_day > 18 then temp
            end
        ) as avg_nighttime_temp
    from hourly_weather
    group by date(timestamp)
) -- Combine all summaries
select coalesce(c.date, h.date, d.date) as date,
    -- Current weather metrics
    c.avg_temp,
    c.min_temp,
    c.max_temp,
    c.avg_feels_like,
    c.avg_humidity,
    c.avg_pressure,
    c.avg_wind_speed,
    c.readings_count,
    c.most_common_description,
    c.most_common_temp_category,
    c.most_common_wind_category,
    -- Hourly weather metrics
    h.avg_temp_hourly,
    h.min_temp_hourly,
    h.max_temp_hourly,
    h.avg_humidity_hourly,
    h.avg_wind_speed_hourly,
    h.avg_precipitation_probability,
    h.hourly_readings_count,
    h.avg_daytime_temp,
    h.avg_nighttime_temp,
    -- Daily forecast metrics
    d.temp_min as forecast_temp_min,
    d.temp_max as forecast_temp_max,
    d.temp_day as forecast_temp_day,
    d.temp_night as forecast_temp_night,
    d.humidity as forecast_humidity,
    d.wind_speed as forecast_wind_speed,
    d.pop as forecast_precipitation_probability,
    d.description as forecast_description,
    d.temp_category as forecast_temp_category,
    d.wind_category as forecast_wind_category,
    d.season,
    -- Derived metrics
    coalesce(c.avg_temp, h.avg_temp_hourly) as final_avg_temp,
    coalesce(c.min_temp, h.min_temp_hourly, d.temp_min) as final_min_temp,
    coalesce(c.max_temp, h.max_temp_hourly, d.temp_max) as final_max_temp,
    case
        when c.avg_temp is not null
        and h.avg_temp_hourly is not null then abs(c.avg_temp - h.avg_temp_hourly)
        else null
    end as temp_variance_current_vs_hourly,
    case
        when h.avg_daytime_temp is not null
        and h.avg_nighttime_temp is not null then h.avg_daytime_temp - h.avg_nighttime_temp
        else null
    end as day_night_temp_difference
from current_daily_summary c
    full outer join hourly_daily_summary h on c.date = h.date
    full outer join daily_weather d on coalesce(c.date, h.date) = d.date