init:
	bash -c "cd infra && bash enable_api.sh && terraform init"

apply:
	bash -c "cd infra && terraform apply"

destroy:
	bash -c "cd infra && terraform destroy"
