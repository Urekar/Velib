USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE gld_station_status
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
SELECT
    station_id,
    num_bikes_available,
    num_docks_available,
    mechanical,
    ebike,
    is_installed,
    is_returning,
    is_renting,
    last_reported,
    station_code,
    file_name,
    load_time,
    TO_TIMESTAMP_NTZ(REGEXP_SUBSTR(file_name, '\\d{8}_\\d{6}'), 'YYYYMMDD_HH24MISS') AS Extract_Date
FROM silver.slv_station_status;