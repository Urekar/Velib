{{ config(
    materialized='incremental',
    unique_key='station_id || last_reported'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'raw_station_status') }}
),

flattened AS (
    SELECT
        s.value:station_id::NUMBER                                    AS station_id,
        s.value:num_bikes_available::NUMBER                           AS num_bikes_available,
        s.value:num_docks_available::NUMBER                           AS num_docks_available,
        s.value:num_bikes_available_types[0].mechanical::NUMBER       AS mechanical,
        s.value:num_bikes_available_types[1].ebike::NUMBER            AS ebike,
        IFF(s.value:is_installed::NUMBER = 1, TRUE, FALSE)            AS is_installed,
        IFF(s.value:is_returning::NUMBER = 1, TRUE, FALSE)            AS is_returning,
        IFF(s.value:is_renting::NUMBER = 1, TRUE, FALSE)              AS is_renting,
        TO_TIMESTAMP_NTZ(s.value:last_reported::NUMBER)               AS last_reported,
        s.value:stationCode::STRING                                   AS station_code,
        r.file_name,
        r.load_time
    FROM {{ source('bronze', 'raw_station_status') }} r,
         LATERAL FLATTEN(input => r.raw:data:stations) AS s
)

SELECT *
FROM flattened

{% if is_incremental() %}
-- Ne charger que les nouvelles données
WHERE last_reported > (SELECT MAX(last_reported) FROM {{ this }})
{% endif %}

