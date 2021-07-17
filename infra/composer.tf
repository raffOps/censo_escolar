resource "google_composer_environment" "composer" {
  name = "airflow"
  region = "us-central1"
    config {
    node_count = 3

    node_config {
      zone         = "us-central1-a"
      machine_type = "e2-small"
    }
      software_config {
        image_version = "composer-1.17.0-preview.2-airflow-2.0.1"
        env_variables = {
          "AIRFLOW_VAR_DATA_LAKE" = google_storage_bucket.data-lake.name
          "AIRFLOW_VAR_FIRST_YEAR" = "2012"
          "AIRFLOW_VAR_LAST_YEAR" = "2014"
          "AIRFLOW_VAR_PROJECT": var.project
        }
      }
  }
}

