//
//data "google_iam_policy" "admin" {
//  binding {
//    role = "roles/storage.admin"
//
//    members = [
//      "user:jane@example.com",
//    ]
//  }
//}
//
//resource "google_service_account" "sa" {
//  account_id   = "my-service-account"
//  display_name = "A service account that only Jane can interact with"
//}
//
//resource "google_service_account_iam_policy" "admin-account-iam" {
//  service_account_id = google_service_account.sa.name
//  policy_data        = data.google_iam_policy.admin.policy_data
//}


//resource "google_service_account" "service_account" {
//  account_id   = "etl-service-account"
//  display_name = "etl"
//}
//
//resource "google_service_account_iam_member" "account-iam" {
//  member = "serviceAccount:${google_service_account.service_account.email}"
//  role = "roles/storage.objectViewer"
//  service_account_id = google_service_account.service_account.name
//}
//
//locals {
//  project = var.project
//}
////
//resource "null_resource" "credentials" {
//  provisioner "local-exec" {
//    command = "gcloud iam service-accounts keys create ../etl/docker-images/extraction/key.json --iam-account=etl-service-account@${local.project}.iam.gserviceaccount.com"
//    interpreter = ["/bin/bash", "-c"]
//  }
//  depends_on = [google_service_account_iam_policy.admin-account-iam]
//}