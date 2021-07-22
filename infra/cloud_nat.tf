//resource "google_compute_router" "router" {
//  name    = "my-router"
//  region  = "southamerica-east1"
//  network = "default"
//}
//
//resource "google_compute_address" "address" {
//  count  = 6
//  name   = "nat-manual-ip-${count.index}"
//  region = "southamerica-east1"
//}
//
//resource "google_compute_router_nat" "nat_manual" {
//  name   = "my-router-nat"
//  router = google_compute_router.router.name
//  region = google_compute_router.router.region
//
//  nat_ip_allocate_option = "MANUAL_ONLY"
//  nat_ips                = google_compute_address.address.*.self_link
//
//  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"
//  subnetwork {
//    name                    = "default"
//    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
//  }
//}