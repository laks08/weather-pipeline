{ { config(materialized = 'view') } } with source as (
    select *
    from { { source('raw', 'current_weather') } }
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
        end as wind_deg_clean
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
    end as wind_category
from cleaned
where timestamp is not null