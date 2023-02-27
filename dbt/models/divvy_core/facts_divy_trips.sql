{{ config(materialized='table') }}

with tripdata as (
    select *, TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS duration_minutes
    from {{ ref('stg_divvy_tripdata') }}
)
SELECT
    ride_id,
    start_station_id,
    end_station_id,
    CASE WHEN membership_status = 'member' THEN 'Annual' ELSE 'casual' END AS membership_status,
    rideable_type,
    start_station_name,
    end_station_name,
    started_at,
    ended_at,
    duration_minutes,
    CONCAT(CAST(start_lat AS STRING), ',', CAST(start_lng AS STRING)) AS start_location,
    CONCAT(CAST(end_lat AS STRING), ',', CAST(end_lng AS STRING)) AS end_location
FROM tripdata
-- total miniutes in a day: 24 * 60 = 1440
-- if duration >= 24 * 60 => invalid trip
WHERE duration_minutes BETWEEN  1 AND 24 * 60 - 1