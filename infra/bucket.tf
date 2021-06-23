data "google_client_config" "default" {
}

resource "google_storage_bucket" "bucket-bronze" {
  name          = "${data.google_client_config.default.project}_bronze"
  location      = "EU"
  force_destroy = false
}

resource "google_storage_bucket" "bucket-silver" {
  name          = "${data.google_client_config.default.project}_silver"
  location      = "EU"
  force_destroy = false
}

resource "google_storage_bucket" "bucket-gold" {
  name          = "${data.google_client_config.default.project}_gold"
  location      = "EU"
  force_destroy = false
}

