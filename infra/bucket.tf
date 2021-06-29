resource "google_storage_bucket" "bucket-bronze" {
  name          = "${var.project}-bronze"
  location      = "EU"
  force_destroy = false
  #depends_on = [null_resource.credentials]
}

resource "google_storage_bucket" "bucket-silver" {
  name          = "${var.project}-silver"
  location      = "EU"
  force_destroy = false
  #depends_on = [null_resource.credentials]
}

resource "google_storage_bucket" "bucket-gold" {
  name          = "${var.project}-gold"
  location      = "EU"
  force_destroy = false
  #depends_on = [null_resource.credentials]
}

