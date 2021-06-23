terraform {
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
    }
    google = {
      source = "hashicorp/google"
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

resource "docker_registry_image" "container-extraction1" {
name = "gcr.io/${var.project}/licitacoes:latest"
build {
context = "../etl/extraction"
dockerfile = "Dockerfile"
  }
}


