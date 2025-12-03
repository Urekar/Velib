CREATE OR REPLACE STORAGE INTEGRATION gcs_velib_integration_v2
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = (
    'gcs://bucket-velib/station_status/',
    'gcs://bucket-velib/station_information/',
    'gcs://bucket-velib/station_details/'
  )
  COMMENT = 'Intégration entre GCS (bucket-velib) et Snowflake';


DESC INTEGRATION gcs_velib_integration_v2;