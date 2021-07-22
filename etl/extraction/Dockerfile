FROM python:3.8
WORKDIR /HOME
RUN apt-get update
RUN apt-get install p7zip-full -y
RUN apt-get install unar -y

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY extract.py .
COPY key.json .
