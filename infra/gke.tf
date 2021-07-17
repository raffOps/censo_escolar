resource "google_container_cluster" "extraction" {
  name = "extraction"
  initial_node_count = 1
  location = "southamerica-east1-a"
  project = var.project
  node_config {
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    machine_type = "e2-small"
  }
  private_cluster_config {
    enable_private_endpoint = false
    enable_private_nodes = true
    master_ipv4_cidr_block = "172.10.0.0/28"
    master_global_access_config {
      enabled = true
    }
  }
  ip_allocation_policy {
    cluster_secondary_range_name = ""
  }
  workload_identity_config {
    identity_namespace = "${var.project}.svc.id.goog"
  }
}

resource "google_container_node_pool" "extraction" {
  name       = "extraction"
  cluster    = google_container_cluster.extraction.name
  initial_node_count = 0
  location = "southamerica-east1-a"

  node_config {
    machine_type = "e2-standard-2"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  autoscaling {
    max_node_count = 20
    min_node_count = 0
  }
}

