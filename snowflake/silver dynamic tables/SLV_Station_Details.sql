USE DATABASE VELIB_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE DYNAMIC TABLE slv_station_details
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    -- Metadata
    r.load_time,
    r.file_name,
    
    -- Extraire date + heure depuis le nom du fichier
    TO_TIMESTAMP_NTZ(
        REGEXP_SUBSTR(r.file_name, '\\d{8}_\\d{4}'),
        'YYYYMMDD_HH24MI'
    ) AS extract_datetime,

    -- Station minimal
    s.value:station:code::string     AS station_id,
    s.value:station:name::string     AS station_name,

    -- Bike details
    b.value:bikeName::string          AS bike_name,
    b.value:bikeElectric::string      AS bike_electric,
    b.value:bikeBatteryLevel::string  AS bike_battery_level,
    b.value:bikeRate::number          AS bike_rate,
    b.value:bikeStatus::string        AS bike_status,
    b.value:dockPosition::string      AS dock_position,
    b.value:lastRateDate::timestamp_ntz AS last_rate_date,
    b.value:numberOfRates::number     AS number_of_rates,
    b.value:stationId::string          AS bike_station_id

FROM BRONZE.brz_station_details r,
     LATERAL FLATTEN(input => r.raw) s,
     LATERAL FLATTEN(input => s.value:bikes) b;