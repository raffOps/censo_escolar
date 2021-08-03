output "composer" {
   value = google_composer_environment.composer.config[0].airflow_uri
}