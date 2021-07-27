resource "docker_registry_image" "censo-escolar_extraction" {
name = "gcr.io/${var.project}/censo_escolar_extraction:latest"
build {
context = "../etl/censo-escolar/extraction"
dockerfile = "Dockerfile"
  }
  depends_on = [null_resource.download_credential_]
}
