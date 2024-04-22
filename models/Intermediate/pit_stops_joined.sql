with base_results as (

    select * from {{ ref('fct_results') }}

), 

pit_stops as (
    select
        race_id,
        driver_id,
        stop_number,
        lap,
        lap_time_formatted,
        pit_stop_duration_seconds,
        pit_stop_milliseconds,
        max(stop_number) over (partition by race_id,driver_id) as total_pit_stops_per_race
    from {{ ref('stg_f1_pit_stops') }}
),

pit_stops_joined as (

    select 
        base_results.race_id,
        race_year,
        base_results.driver_id,
        constructor_id,
        constructor_name,
        stop_number,
        lap, 
        lap_time_formatted,
        pit_stop_duration_seconds, 
        pit_stop_milliseconds
    from base_results
    left join pit_stops
        on base_results.race_id=pit_stops.race_id and base_results.driver_id=pit_stops.driver_id
)
select * from pit_stops_joined