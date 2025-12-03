USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE GLD_Station_Information
TARGET_LAG = DOWNSTREAM
WAREHOUSE = Velib_GOLD_WH
AS
SELECT
    station_id,
    station_code,
    station_name,
    latitude,
    longiture,
    capacity,
    file_name,
    load_time
FROM silver.SLV_STATION_INFORMATION;