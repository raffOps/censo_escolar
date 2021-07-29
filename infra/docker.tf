resource "docker_registry_image" "censo_escolar_extraction" {
name = "gcr.io/${var.project}/censo_escolar_extraction:latest"
build {
context = "../etl/censo_escolar/extraction"
dockerfile = "Dockerfile"
  }
  depends_on = [null_resource.download_credential]
}
