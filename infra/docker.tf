resource "docker_registry_image" "container-extraction" {
name = "gcr.io/${var.project}/censo_escolar:latest"
build {
context = "../etl/docker-images/extraction"
dockerfile = "Dockerfile"
  }
  #depends_on = [null_resource.credentials]
}
