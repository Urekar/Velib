USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE DIM_STATION
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
WITH ranked AS (
    SELECT
        station_id,
        station_code,
        station_name,
        latitude,
        longitude,
        capacity,
        load_time,
        ROW_NUMBER() OVER (
            PARTITION BY station_id 
            ORDER BY load_time DESC
        ) AS rn
    FROM silver.SLV_STATION_INFORMATION
)
SELECT
    station_id,
    station_code,
    station_name,
    latitude,
    longitude,
    capacity,
    load_time
FROM ranked
WHERE rn = 1;
