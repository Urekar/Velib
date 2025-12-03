USE DATABASE VELIB_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE PIPE pipe_station_status
AUTO_INGEST = TRUE
INTEGRATION = gcs_int
AS
COPY INTO raw_station_status (raw, file_name)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @stage_station_status
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*\\.json$';