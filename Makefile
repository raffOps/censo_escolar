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
	echo \
	  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
	  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
	sudo apt-get update
	sudo apt-get install docker-ce docker-compose docker-ce-cli containerd.io
	sudo groupadd docker
	sudo usermod -aG docker $USER
	sudo docker run hello-world

gcp:
	sudo echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
	sudo apt-get install apt-transport-https ca-certificates gnupg -y
	sudo curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
	sudo apt-get update && apt-get install google-cloud-sdk -y


#service_account:
#	gcloud iam service-accounts create docker-rais --display-name="docker_rais"
#	gcloud iam service-accounts keys create key.json --iam-account docker-rais@ <<PROJETO>>.iam.gserviceaccount.com
