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
          AIRFLOW_VAR_CENSO_ESCOLAR_FIRST_YEAR: 2011
          AIRFLOW_VAR_CENSO_ESCOLAR_LAST_YEAR: 2020
          AIRFLOW_VAR_PROJECT: var.project
        }
      }
  }
}

