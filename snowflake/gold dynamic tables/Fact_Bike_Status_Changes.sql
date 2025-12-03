USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE GLD_BIKE_MOVES
TARGET_LAG = DOWNSTREAM
WAREHOUSE = VELIB_GOLD_WH
AS
WITH base AS (
    SELECT
        BIKE_NAME,
        STATION_ID,
        STATION_NAME,
        EXTRACT_DATETIME
    FROM silver.SLV_STATION_DETAILS
),

-- 📌 Liste des timestamps disponibles
valid_minutes AS (
    SELECT DISTINCT EXTRACT_DATETIME
    FROM base
),

-- 📌 On ne garde QUE les paires T → T+1 qui existent vraiment
valid_pairs AS (
    SELECT
        v.EXTRACT_DATETIME AS t0,
        DATEADD(minute, 1, v.EXTRACT_DATETIME) AS t1
    FROM valid_minutes v
    INNER JOIN valid_minutes v2
        ON v2.EXTRACT_DATETIME = DATEADD(minute, 1, v.EXTRACT_DATETIME)
),

-- 🚲 Détections OUT = présent à T0 mais absent à T1
outs AS (
    SELECT
        b.BIKE_NAME,
        b.STATION_ID,
        b.STATION_NAME,
        'OUT' AS STATUS,
        b.EXTRACT_DATETIME AS EVENT_TIME
    FROM base b
    INNER JOIN valid_pairs p ON p.t0 = b.EXTRACT_DATETIME
    LEFT JOIN base b2
        ON b.BIKE_NAME = b2.BIKE_NAME
       AND b2.EXTRACT_DATETIME = p.t1
    WHERE b2.BIKE_NAME IS NULL
),

-- 🚲 Détections IN = absent à T0 mais présent à T1
ins AS (
    SELECT
        b2.BIKE_NAME,
        b2.STATION_ID,
        b2.STATION_NAME,
        'IN' AS STATUS,
        b2.EXTRACT_DATETIME AS EVENT_TIME
    FROM base b2
    INNER JOIN valid_pairs p ON p.t1 = b2.EXTRACT_DATETIME
    LEFT JOIN base b
        ON b2.BIKE_NAME = b.BIKE_NAME
       AND b.EXTRACT_DATETIME = p.t0
    WHERE b.BIKE_NAME IS NULL
)

SELECT * FROM outs
UNION ALL
SELECT * FROM ins;