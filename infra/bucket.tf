resource "google_storage_bucket" "data-lake" {
  name          = "${var.project}"
  location      = "US"
  force_destroy = false
}

resource "null_resource" "upload_files_" {
    provisioner "local-exec" {
        command = "gsutil cp -r ../etl/* gs://${google_storage_bucket.data-lake.name}/etl"
        interpreter = ["/bin/bash", "-c"]
    }
}

