#!/bin/bash
cat api.txt | while read line
	do
	   gcloud services enable $line
	   echo "$line enabled"
	done