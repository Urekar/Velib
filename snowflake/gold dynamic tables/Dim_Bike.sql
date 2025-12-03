USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE DIM_BIKE
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
SELECT
    bike_name AS bike_id,
    bike_electric AS is_electric,
    bike_status,
    bike_rate,
    number_of_rates,
    last_rate_date,
    load_time
FROM (
    SELECT 
        bike_name,
        bike_electric,
        bike_status,
        bike_rate,
        number_of_rates,
        last_rate_date,
        load_time,
        ROW_NUMBER() OVER (
            PARTITION BY bike_name ORDER BY load_time DESC
        ) AS rn
    FROM silver.SLV_STATION_DETAILS
)
WHERE rn = 1;
