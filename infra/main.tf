terraform {
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
      version = "2.13.0"
    }
    google = {
      source = "hashicorp/google"
      version = "3.73.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = "us-central1"
  zone        = "us-central1-a"
}

provider "docker" {

  registry_auth {
    address = "gcr.io"
    config_file = pathexpand("~/.docker/config.json")
  }
}
