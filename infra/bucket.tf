resource "google_storage_bucket" "data-lake" {
    for_each = toset( ["landing", "processing", "consumer", "etl"] )
  name          = "${var.project}-${each.key}"
  location      = "US"
  force_destroy = false
}

