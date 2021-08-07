resource "docker_registry_image" "censo_escolar_extraction" {
name = "gcr.io/${var.project}/censo_escolar_extraction:latest"
build {
context = "../etl/censo_escolar/extract"
dockerfile = "Dockerfile"
  }
  depends_on = [null_resource.download_credential]
}

# resource "null_resource" "download_credential" {
#   provisioner "local-exec" {
#     command = "rm ../etl/censo_escolar/extract/key.json"
#     interpreter = ["/bin/bash", "-c"]
#   }
#   depends_on = [docker_registry_image.censo_escolar_extraction]
# }
