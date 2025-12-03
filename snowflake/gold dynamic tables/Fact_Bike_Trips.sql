USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE FACT_BIKE_TRIPS
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
WITH bike_snapshots AS (
    SELECT
        bike_name AS bike_id,
        station_id AS station_current_id,
        station_name AS station_current_name,
        extract_datetime,
        LEAD(station_id) OVER (PARTITION BY bike_name ORDER BY extract_datetime) AS station_next_id,
        LEAD(station_name) OVER (PARTITION BY bike_name ORDER BY extract_datetime) AS station_next_name,
        LEAD(extract_datetime) OVER (PARTITION BY bike_name ORDER BY extract_datetime) AS timestamp_next
    FROM silver.SLV_STATION_DETAILs
),
bike_trips AS (
    SELECT
        bike_id,
        station_current_id AS station_depart_id,
        station_current_name AS station_depart_name,
        station_next_id AS station_arrivee_id,
        station_next_name AS station_arrivee_name,
        extract_datetime AS date_depart,
        timestamp_next AS date_arrivee,
        DATEDIFF('minute', extract_datetime, timestamp_next) AS temps_trajet_min
    FROM bike_snapshots
    WHERE station_next_id IS NOT NULL
      -- on garde seulement les vélos qui ont été décrochés et racrochés
      AND station_current_id != station_next_id
)
SELECT *
FROM bike_trips;