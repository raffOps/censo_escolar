resource "google_dataproc_cluster" "mycluster" {
  provider = google-beta

  name     = "dados-abertos"
  region   = "us-central1"
//  graceful_decommission_timeout = "120s"
//  labels = {
//    foo = "bar"
//  }

  cluster_config {
    #staging_bucket = "dataproc-staging-bucket"

    master_config {
      num_instances = 1
      machine_type  = "n1-highmem-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances    = 4
      machine_type     =  "n1-highmem-4"
      #min_cpu_platform = "Intel Skylake"
      disk_config {
        boot_disk_size_gb = 30
        num_local_ssds    = 1
      }
    }

//    preemptible_worker_config {
//      num_instances = 0
//    }

    # Override or set some custom properties
    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER"]
    }

    endpoint_config {
      enable_http_port_access = true
    }

    gce_cluster_config {
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = google_service_account.service_account.email
      service_account_scopes = [
        "cloud-platform"
      ]
    }
        # You can define multiple initialization_action blocks
//    initialization_action {
//      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
//      timeout_sec = 500
//    }
  }
  depends_on = [google_project_iam_member.members]
}