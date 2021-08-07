resource "google_bigquery_dataset" "censo_escolar" {
  dataset_id                  = "censo_escolar"
  friendly_name               = "censo_escolar"
  location                    = "US"
}
