terraform:
	wget https://releases.hashicorp.com/terraform/1.0.0/terraform_1.0.0_linux_amd64.zip
	unzip terraform_1.0.0_linux_amd64.zip
	rm terraform_1.0.0_linux_amd64.zip
	sudo mv terraform /usr/local/bin/

docker:
	sudo apt-get update
	sudo apt-get install \
				apt-transport-https \
				ca-certificates \
				curl \
				gnupg \
				lsb-release
	sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
init:
	bash -c "cd infra && terraform init"

apply:
	bash -c "cd infra && terraform apply"
