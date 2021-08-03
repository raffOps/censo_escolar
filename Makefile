init:
	bash -c "cd infra && bash enable_api.sh"
	bash -c "cd infra && terraform init"

apply:
	bash -c "cd infra && terraform apply"

destroy:
	bash -c "cd infra && terraform destroy"
