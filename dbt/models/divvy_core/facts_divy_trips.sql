{{ config(materialized='table') }}

WITH tripdata AS (
    SELECT 
        ride_id, 
        rideable_type,
        membership_status,
        start_station_name,
        end_station_name,
        started_at,
        ended_at,
        TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS duration_minutes
    FROM {{ ref('stg_divvy_tripdata') }}
),
neighborhoods AS (
    SELECT 
        station_id,
        station_name,
        CONCAT(CAST(lat AS STRING), ',', CAST(lng AS STRING)) AS location,
        primary_neighborhood AS neighborhood
    FROM {{ ref('dim_neighborhoods') }}

)
SELECT 
    ride_id, 
    rideable_type,
    membership_status,
    start_station_info.station_id AS start_station_id,
    start_station_name,
    end_station_name,
    end_station_info.station_id AS end_station_id,
    started_at,
    ended_at,
    duration_minutes,
    start_station_info.neighborhood AS start_neighborhood,
    start_station_info.location AS start_location,
    end_station_info.neighborhood AS end_neighborhood,
    end_station_info.location AS end_location
FROM tripdata 
INNER JOIN  neighborhoods AS start_station_info
ON tripdata.start_station_name = start_station_info.station_name
INNER JOIN neighborhoods AS end_station_info
ON tripdata.end_station_name = end_station_info.station_name
WHERE duration_minutes BETWEEN  1 AND 24 * 60 - 1