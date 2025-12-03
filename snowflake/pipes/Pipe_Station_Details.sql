USE DATABASE VELIB_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE PIPE pipe_station_details
AUTO_INGEST = TRUE
INTEGRATION = gcs_int
AS
COPY INTO raw_station_details (raw, file_name)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @stage_station_details
)
FILE_FORMAT = (TYPE = 'JSON', COMPRESSION = 'AUTO')
PATTERN = '.*\\.json(\\.gz)?$';