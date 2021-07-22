#!/usr/bin/bash

docker build -t censo_escolar .
docker tag censo_escolar gcr.io/rjr-portal-da-transparencia/censo_escolar:latest
docker push gcr.io/rjr-portal-da-transparencia/censo_escolar:latest