{ { config(materialized = 'view') } } with source as (
    select *
    from { { source('raw', 'daily_weather') } }
),
cleaned as (
    select date,
        temp_min,
        temp_max,
        temp_day,
        temp_night,
        humidity,
        pressure,
        wind_speed,
        wind_deg,
        description,
        icon,
        pop,
        -- Add data quality checks
        case
            when temp_min between -50 and 50 then temp_min
            else null
        end as temp_min_clean,
        case
            when temp_max between -50 and 50 then temp_max
            else null
        end as temp_max_clean,
        case
            when temp_day between -50 and 50 then temp_day
            else null
        end as temp_day_clean,
        case
            when temp_night between -50 and 50 then temp_night
            else null
        end as temp_night_clean,
        case
            when humidity between 0 and 100 then humidity
            else null
        end as humidity_clean,
        case
            when pressure between 800 and 1200 then pressure
            else null
        end as pressure_clean,
        case
            when wind_speed >= 0 then wind_speed
            else null
        end as wind_speed_clean,
        case
            when wind_deg between 0 and 360 then wind_deg
            else null
        end as wind_deg_clean,
        case
            when pop between 0 and 1 then pop
            else null
        end as pop_clean
    from source
)
select date,
    temp_min_clean as temp_min,
    temp_max_clean as temp_max,
    temp_day_clean as temp_day,
    temp_night_clean as temp_night,
    humidity_clean as humidity,
    pressure_clean as pressure,
    wind_speed_clean as wind_speed,
    wind_deg_clean as wind_deg,
    description,
    icon,
    pop_clean as pop,
    -- Add derived fields
    (temp_max_clean + temp_min_clean) / 2 as temp_avg,
    temp_max_clean - temp_min_clean as temp_range,
    case
        when temp_avg < 0 then 'freezing'
        when temp_avg < 10 then 'cold'
        when temp_avg < 20 then 'cool'
        when temp_avg < 30 then 'warm'
        else 'hot'
    end as temp_category,
    case
        when wind_speed_clean < 5 then 'light'
        when wind_speed_clean < 15 then 'moderate'
        when wind_speed_clean < 25 then 'strong'
        else 'very_strong'
    end as wind_category,
    case
        when pop_clean < 0.1 then 'low'
        when pop_clean < 0.5 then 'medium'
        else 'high'
    end as precipitation_probability,
    -- Extract day of week
    extract(
        dow
        from date
    ) as day_of_week,
    -- Extract month
    extract(
        month
        from date
    ) as month,
    -- Extract season
    case
        when extract(
            month
            from date
        ) in (12, 1, 2) then 'winter'
        when extract(
            month
            from date
        ) in (3, 4, 5) then 'spring'
        when extract(
            month
            from date
        ) in (6, 7, 8) then 'summer'
        else 'fall'
    end as season
from cleaned
where date is not null