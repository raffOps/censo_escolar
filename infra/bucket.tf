resource "google_storage_bucket" "scripts" {
      name          = "${var.project}-scripts"
      location      = "US"
      force_destroy = false
}

resource "google_storage_bucket" "landing" {
      name          = "${var.project}-landing"
      location      = "US"
      force_destroy = false
}


resource "google_storage_bucket" "processing" {
      name          = "${var.project}-processing"
      location      = "US"
      force_destroy = false
}


resource "google_storage_bucket" "consumer" {
      name          = "${var.project}-consumer"
      location      = "US"
      force_destroy = false
}

