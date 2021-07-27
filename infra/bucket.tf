resource "google_storage_bucket" "data-lake" {
  name          = "${var.project}"
  location      = "US"
  force_destroy = false
}

