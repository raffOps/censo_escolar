output "bucket_bronze" {
  value = google_storage_bucket.bucket-bronze.url
}

output "bucket_silver" {
  value = google_storage_bucket.bucket-silver.url
}

output "bucket_gold" {
  value = google_storage_bucket.bucket-gold.url
}

//output "composer" {
//  value = google_composer_environment.composer.config[0].airflow_uri
//}