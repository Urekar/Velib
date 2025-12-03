USE DATABASE VELIB_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE PIPE pipe_station_information
AUTO_INGEST = TRUE
INTEGRATION = gcs_int
AS
COPY INTO raw_station_information (raw, file_name)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @stage_station_information
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*\\.json$';