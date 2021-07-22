resource "google_cloudbuild_trigger" "git_sync" {
  name = "git-sync"
  github {
    owner = var.git_user
    name = "etl_censo_escolar"
    push {
      branch = "dev"
    }
  }

    substitutions = {
    _GCS_BUCKET = google_composer_environment.composer.config[0].dag_gcs_prefix
  }
  filename = "infra/git_sync.yaml"
}