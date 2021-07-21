resource "docker_registry_image" "container-extraction_" {
name = "gcr.io/${var.project}/censo_escolar:latest"
build {
context = "../etl/extraction"
dockerfile = "Dockerfile"
  }
  depends_on = [null_resource.download_credential]
}
