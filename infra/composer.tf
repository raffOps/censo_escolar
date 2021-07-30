resource "google_composer_environment" "composer" {
  name = "airflow"
  region = "us-central1"
    config {
    node_count = 3

    node_config {
      zone         = "us-central1-a"
      machine_type = "e2-medium"
    }
      software_config {
        image_version = "composer-1.17.0-preview.2-airflow-2.0.1"
        env_variables = {
          
          AIRFLOW_VAR_DATA_LAKE_LANDING: google_storage_bucket.landing.name
          AIRFLOW_VAR_DATA_LAKE_PROCESSING: google_storage_bucket.processing.name
          AIRFLOW_VAR_DATA_LAKE_CONSUMER: google_storage_bucket.consumer.name
          AIRFLOW_VAR_DATA_LAKE_SCRIPTS: google_storage_bucket.scripts.name

          AIRFLOW_VAR_FIRST_YEAR: 2011
          AIRFLOW_VAR_LAST_YEAR: 2020
          AIRFLOW_VAR_PROJECT: var.project
        }
      }
  }
}

