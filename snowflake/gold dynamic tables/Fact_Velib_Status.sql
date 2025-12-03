USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE FACT_VELIB_STATUS
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
SELECT
    sd.bike_name as bike_id,
    sd.station_id,
    sd.bike_station_id as station_code,
    sd.dock_position,
    sd.bike_status,
    sd.extract_datetime,
    TIMEFROMPARTS(EXTRACT(HOUR FROM extract_datetime), EXTRACT(MINUTE FROM extract_datetime), 0, 0) AS time_value,
    TO_DATE(extract_datetime) AS date_value,
TO_NUMBER(TO_CHAR(TO_DATE(extract_datetime), 'YYYYMMDD')) AS date_key

FROM silver.SLV_STATION_DETAILS sd