{{ config(materialized='view') }}

WITH tripdata AS (
    SELECT * 
    FROM {{ source('staging','divvy_tripdata') }}
     WHERE
    (ride_id IS NOT NULL AND ride_id <> 'nan') AND 
    (rideable_type IS NOT NULL AND rideable_type <> 'nan') AND 
    (start_station_name IS NOT NULL AND start_station_name <> 'nan') AND  
    (end_station_name IS NOT NULL AND end_station_name <> 'nan') AND 
    (start_station_id IS NOT NULL AND start_station_id <> 'nan') AND  
    (end_station_id IS NOT NULL AND end_station_id <> 'nan') AND 
    (member_casual IS NOT NULL AND member_casual <> 'nan') AND 
    started_at IS NOT NULL AND  
    ended_at IS NOT NULL AND
    start_lat IS NOT NULL AND
    start_lng IS NOT NULL AND 
    end_lat IS NOT NULL AND  
    end_lng IS NOT NULL 
)

SELECT 
    -- ride info
    CAST(ride_id AS STRING) AS ride_id,
    CAST(rideable_type AS STRING) AS rideable_type,
    CAST(member_casual AS STRING) AS membership_status,

    -- Timestamp
    CAST(started_at AS TIMESTAMP) AS started_at, 
    CAST(ended_at AS TIMESTAMP) AS ended_at,

    -- station info
    CAST(start_station_name AS STRING) AS start_station_name,
    CAST(start_station_id AS STRING) AS start_station_id,
    CAST(end_station_name AS STRING) AS end_station_name,
    CAST(end_station_id AS STRING) AS end_station_id,

    -- Station info: geo-spatial convert to geospatial in google-studio
    CAST(start_lat AS NUMERIC) AS start_lat, 
    CAST(start_lng AS NUMERIC) AS start_lng, 
    CAST(end_lat AS NUMERIC) AS end_lat, 
    CAST(end_lng AS NUMERIC) AS end_lng 

FROM tripdata