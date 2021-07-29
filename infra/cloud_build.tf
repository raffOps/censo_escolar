resource "google_cloudbuild_trigger" "update_dags" {
  name = "updata_dags"
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
  filename = "infra/update_dags.yaml"
}


resource "google_cloudbuild_trigger" "update_etl_bucket" {
  name = "update_etl_bucket"
  github {
    owner = var.git_user
    name = "etl_censo_escolar"
    push {
      branch = "dev"
    }
  }

    substitutions = {
    _GCS_BUCKET = "${google_storage_bucket.data-lake.name}-etl"
  }
  filename = "infra/update_etl_bucket.yaml"
}