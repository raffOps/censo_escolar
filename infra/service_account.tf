# create service accounts
resource "google_service_account" "service_account" {
  account_id = "etl-service-account"
  display_name = "etl-service-account"
  project      = var.project
}

resource "google_project_iam_member" "members" {
    for_each = toset(["roles/storage.admin", "roles/dataproc.worker"])
        project            = var.project
        role               = each.key
        member             = "serviceAccount:${google_service_account.service_account.email}"
}


resource "null_resource" "download_credential" {
  provisioner "local-exec" {
    command = "gcloud iam service-accounts keys create ../etl/censo_escolar/extract/key.json --iam-account=etl-service-account@${var.project}.iam.gserviceaccount.com"
    interpreter = ["/bin/bash", "-c"]
  }
  depends_on = [google_project_iam_member.members]
}

