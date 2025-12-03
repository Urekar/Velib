USE DATABASE VELIB_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DIM_TIME AS
WITH minutes AS (
    SELECT
        SEQ4() AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 1440)) -- 24*60 minutes pour une journée
)
SELECT
    seq AS time_id,
    TIMEFROMPARTS(FLOOR(seq/60), MOD(seq,60), 0, 0) AS time_value,
    FLOOR(seq/60) AS hour,
    MOD(seq,60) AS minute,
    FLOOR(seq/15) + 1 AS quarter_hour,
    CASE 
        WHEN FLOOR(seq/60) BETWEEN 0 AND 5 THEN 'Nuit'
        WHEN FLOOR(seq/60) BETWEEN 6 AND 11 THEN 'Matin'
        WHEN FLOOR(seq/60) BETWEEN 12 AND 17 THEN 'Après-midi'
        ELSE 'Soir'
    END AS time_segment
FROM minutes;