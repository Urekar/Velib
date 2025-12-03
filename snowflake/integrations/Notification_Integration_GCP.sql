USE DATABASE VELIB_DB;

CREATE NOTIFICATION INTEGRATION gcs_int          -- Nom de l’intégration dans Snowflake
  TYPE = QUEUE                                   -- Mode file d’attente (pour recevoir des messages)
  ENABLED = TRUE                                 -- Active l’intégration
  NOTIFICATION_PROVIDER = GCP_PUBSUB             -- Le provider : ici Pub/Sub de Google Cloud
  DIRECTION = INBOUND                            -- Snowflake va RECEVOIR des messages de GCP
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/projet-velib-474009/subscriptions/velib_notifications_sub';  -- Nom complet de la subscription Pub/Sub à écouter                                               


DESC INTEGRATION gcs_int;