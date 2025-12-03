USE DATABASE VELIB_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE DYNAMIC TABLE SLV_Station_Information
TARGET_LAG = '12 HOURS'
WAREHOUSE = Velib_Silver_WH
AS
SELECT
    s.value:station_id::NUMBER AS station_id,
    s.value:stationCode::STRING AS station_code,
    s.value:name::STRING AS station_name,
    s.value:lat::FLOAT AS latitude,
    s.value:lon::FLOAT AS longitude,
    s.value:capacity::NUMBER AS capacity,
    r.file_name,
    r.load_time
FROM bronze.raw_station_information AS r,
     LATERAL FLATTEN(input => r.raw:data:stations) AS s;