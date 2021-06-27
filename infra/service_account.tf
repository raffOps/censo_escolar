resource "google_service_account" "service_account" {
  account_id   = "etl-service-account"
  display_name = "etl"
}

resource "google_service_account_iam_member" "account-iam" {
  member = "serviceAccount:${google_service_account.service_account.email}"
  role = "roles/owner"
  service_account_id = google_service_account.service_account.name
}