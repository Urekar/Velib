{{ config(
    materialized='incremental',
    unique_key='station_id || file_name'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'brz_station_information') }}
),

flattened AS (
    SELECT
        s.value:station_id::NUMBER      AS station_id,
        s.value:stationCode::STRING     AS station_code,
        s.value:name::STRING            AS station_name,
        s.value:lat::FLOAT              AS latitude,
        s.value:lon::FLOAT              AS longitude,
        s.value:capacity::NUMBER        AS capacity,
        r.file_name,
        r.load_time
    FROM source r,
         LATERAL FLATTEN(input => r.raw:data:stations) AS s
)

SELECT *
FROM flattened

{% if is_incremental() %}
-- Ne charger que les lignes nouvelles
WHERE file_name || station_id NOT IN (
    SELECT file_name || station_id FROM {{ this }}
)
{% endif %}

