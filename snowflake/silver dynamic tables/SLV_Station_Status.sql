USE DATABASE VELIB_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE DYNAMIC TABLE SLV_STATION_STATUS
TARGET_LAG = '12 HOURS'
WAREHOUSE = Velib_Silver_WH
AS
SELECT
    s.value:station_id::NUMBER AS station_id,
    s.value:num_bikes_available::NUMBER AS num_bikes_available,
    s.value:num_docks_available::NUMBER AS num_docks_available,
    s.value:num_bikes_available_types[0].mechanical::NUMBER AS mechanical,
    s.value:num_bikes_available_types[1].ebike::NUMBER AS ebike,
    s.value:is_installed::NUMBER = 1 AS is_installed,
    s.value:is_returning::NUMBER = 1 AS is_returning,
    s.value:is_renting::NUMBER = 1 AS is_renting,
    TO_TIMESTAMP_NTZ(s.value:last_reported::NUMBER) AS last_reported,
    s.value:stationCode::STRING AS station_code,
    r.file_name,
    r.load_time
FROM bronze.brz_station_status AS r,
     LATERAL FLATTEN(input => r.raw:data:stations) AS s;