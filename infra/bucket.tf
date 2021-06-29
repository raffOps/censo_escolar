resource "google_storage_bucket" "data-lake" {
  name          = "${var.project}"
  location      = "EU"
  force_destroy = false
  #depends_on = [null_resource.credentials]
}

