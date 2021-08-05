# resource "google_dataproc_cluster" "mycluster" {
#   provider = google-beta

#   name     = "dados-abertos"
#   region   = "us-central1"

#   cluster_config {
#     master_config {
#       num_instances = 1
#       machine_type  = "n1-highmem-8"
#       disk_config {
#         boot_disk_type    = "pd-ssd"
#         boot_disk_size_gb = 100
#       }
#     }

#     worker_config {
#       num_instances    = 2
#       machine_type     =  "n1-highmem-8"
#       disk_config {
#         boot_disk_size_gb = 100
#         num_local_ssds    = 1
#       }
#     }

#     software_config {
#       image_version = "2.0-debian10"
#       override_properties = {
#         "dataproc:dataproc.allow.zero.workers" = "true"
#       }
#       optional_components = ["JUPYTER"]
#     }

#     endpoint_config {
#       enable_http_port_access = true
#     }

#     gce_cluster_config {
#       service_account = google_service_account.service_account.email
#       service_account_scopes = [
#         "cloud-platform"
#       ]
#     }
#   }
#   depends_on = [google_project_iam_member.members]
# }


//resource "google_dataproc_workflow_template" "censo-escolar-transform" {
//  name = "censo-escolar-transform"
//  location = "us-east1"
//  placement {
//    managed_cluster {
//      cluster_name = "censo-escolar"
//      config {
//        gce_cluster_config {
//          zone = "us-east1-b"
//        }
//        master_config {
//          num_instances = 1
//          machine_type = "n1-highmem-8"
//          disk_config {
//            boot_disk_type = "pd-ssd"
//            boot_disk_size_gb = 100
//          }
//        }
//        worker_config {
//          num_instances = 2
//          machine_type = "n1-highmem-8"
//          disk_config {
//            boot_disk_size_gb = 100
//            num_local_ssds = 2
//          }
//        }
//
//        software_config {
//          image_version = "2.0-debian10"
//        }
//      }
//    }
//  }
//  jobs {
//    step_id = "pyspark"
//    pyspark_job {
//      main_python_file_uri = "gs://${google_storage_bucket.scripts.name}/censo_escolar/transform/transform.py"
//      args = ["project", "year"]
//    }
//  }
//  parameters {
//    name = "PROJECT"
//    fields = ["jobs['pyspark'].pysparkJob.args[0]"]
//  }
//
//  parameters {
//    name = "YEAR"
//    fields = ["jobs['pyspark'].pysparkJob.args[1]"]
//  }
//}