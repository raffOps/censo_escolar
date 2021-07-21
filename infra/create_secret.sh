gcloud container clusters get-credentials $2 --zone us-central1-a --project $1
gcloud iam service-accounts keys create key.json --iam-account=etl-service-account@$1.iam.gserviceaccount.com
kubectl create secret generic gcs-credentials --from-file ./key.json
rm key.json