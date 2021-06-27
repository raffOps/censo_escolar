dev:
	echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
	docker-compose up airflow-init
	docker-compose up -d

update:
	docker-compose up -d

clean:
	docker-compose down --volumes --rmi all