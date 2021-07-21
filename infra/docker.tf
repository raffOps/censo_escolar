resource "docker_registry_image" "container-extraction" {
name = "gcr.io/${var.project}/censo_escolar:latest"
build {
context = "../etl/extraction"
dockerfile = "Dockerfile"
  }
}
