{ { config(materialized = 'view') } } with source as (
    select *
    from { { source('raw', 'hourly_weather') } }
),
cleaned as (
    select timestamp,
        temp,
        feels_like,
        humidity,
        pressure,
        wind_speed,
        wind_deg,
        description,
        icon,
        pop,
        -- Add data quality checks
        case
            when temp between -50 and 50 then temp
            else null
        end as temp_clean,
        case
            when feels_like between -50 and 50 then feels_like
            else null
        end as feels_like_clean,
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
select timestamp,
    temp_clean as temp,
    feels_like_clean as feels_like,
    humidity_clean as humidity,
    pressure_clean as pressure,
    wind_speed_clean as wind_speed,
    wind_deg_clean as wind_deg,
    description,
    icon,
    pop_clean as pop,
    -- Add derived fields
    case
        when temp_clean < 0 then 'freezing'
        when temp_clean < 10 then 'cold'
        when temp_clean < 20 then 'cool'
        when temp_clean < 30 then 'warm'
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
    -- Extract hour for analysis
    extract(
        hour
        from timestamp
    ) as hour_of_day,
    -- Extract day of week
    extract(
        dow
        from timestamp
    ) as day_of_week
from cleaned
where timestamp is not null