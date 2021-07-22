output "data_lake" {
  value = google_storage_bucket.data-lake.url
}

output "composer" {
  value = google_composer_environment.composer.config[0].airflow_uri
}