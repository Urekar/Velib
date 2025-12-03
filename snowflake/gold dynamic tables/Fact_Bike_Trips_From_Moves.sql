USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE VELIB_DB.GOLD.FACT_BIKE_TRIPS_FROM_MOVES(
    BIKE_ID,
    STATION_DEPART_ID,
    STATION_DEPART_NAME,
    STATION_ARRIVEE_ID,
    STATION_ARRIVEE_NAME,
    DATE_DEPART,
    DATE_ARRIVEE,
    TEMPS_TRAJET_MIN
)
TARGET_LAG = 'DOWNSTREAM'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = VELIB_GOLD_WH
AS
WITH ordered_moves AS (
    SELECT
        BIKE_NAME,
        STATION_ID,
        STATION_NAME,
        STATUS,
        EVENT_TIME,
        ROW_NUMBER() OVER (
            PARTITION BY BIKE_NAME 
            ORDER BY EVENT_TIME
        ) AS rn
    FROM GLD_BIKE_MOVES
),
-- On associe chaque OUT à l’IN suivant pour le même vélo
pair_trips AS (
    SELECT
        out_move.BIKE_NAME AS BIKE_ID,
        out_move.STATION_ID AS STATION_DEPART_ID,
        out_move.STATION_NAME AS STATION_DEPART_NAME,
        in_move.STATION_ID AS STATION_ARRIVEE_ID,
        in_move.STATION_NAME AS STATION_ARRIVEE_NAME,
        out_move.EVENT_TIME AS DATE_DEPART,
        in_move.EVENT_TIME AS DATE_ARRIVEE,
        DATEDIFF('minute', out_move.EVENT_TIME, in_move.EVENT_TIME) AS TEMPS_TRAJET_MIN
    FROM ordered_moves out_move
    JOIN ordered_moves in_move
        ON out_move.BIKE_NAME = in_move.BIKE_NAME
       AND out_move.STATUS = 'OUT'
       AND in_move.STATUS = 'IN'
       AND in_move.EVENT_TIME > out_move.EVENT_TIME
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY out_move.BIKE_NAME, out_move.EVENT_TIME
        ORDER BY in_move.EVENT_TIME
    ) = 1
)
SELECT *
FROM pair_trips
WHERE TEMPS_TRAJET_MIN > 0;