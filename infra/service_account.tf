# create service accounts
resource "google_service_account" "service_account" {
  account_id = "extraction"
  display_name = "extraction"
  project      = var.project
}

# conditionally assign billing user role on a specific billing account
resource "google_project_iam_member" "billing_user" {
  project = var.project
  role               = "roles/storage.admin"
  member             = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_service_account_key" "keys" {
  service_account_id = google_service_account.service_account.email
}
