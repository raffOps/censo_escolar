from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator
)

from google.cloud import storage

BUCKET_BRONZE = Variable.get("BUCKET_BRONZE")
BUCKET_SILVER = Variable.get("BUCKET_SILVER")
BUCKET_GOLD = Variable.get("BUCKET_GOLD")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))


def get_cluster_config():
    cpu = {
        "resourceType": "cpu",
        "minimum": "1",
        "maximum": "6"
    }
    memory = {
        "resourceType": "memory",
        "minimum": "4",
        "maximum": "24"
    }

    cluster_auto_scaling = {
        "resourceLimits": [cpu, memory],
        "enableNodeAutoprovisioning": True
    }
    vertical_pod_autoscaling = {"enabled": True}

    node_config = {"oauthScopes": ["https://www.googleapis.com/auth/cloud-platform"]}

    cluster_config = {
        "name": "extract-cluster",
        "initialNodeCount": 2,
        "autoscaling": cluster_auto_scaling,
        "verticalPodAutoscaling": vertical_pod_autoscaling,
        "location": "us-central1-a",
        "nodeConfig": node_config
    }
    return cluster_config


def get_secret():
    secret = Secret(
        deploy_type='volume',
        deploy_target='/var/secrets/google',
        secret='service-account-extracao',
        key='service-account.json')
    return secret


def check_files(**context):
    ti = context["ti"]
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_BRONZE)
    years = bucket.list_blobs(delimiter="/")
    if years:
        ti.xcom_push(key="folders_in_bronze_bucket", value=years)
        return "create-gke-cluster"
    else:
        return "check-silver-bucket"


def extract_file_error_callback(**context):
    instance = context["task_instance"]
    print(f"Error extraction: {instance}")
    ti = context["ti"]
    ti.xcom_push(key="failed_extraction", value=True)


def check_extraction(**context):
    ti = context["ti"]
    any_failure = ti.xcom_pull(key="failed extraction")
    if any_failure:
        return "some-failed-extraction"
    else:
        return "check-silver-bucket"


def raise_exception_operator():
    raise Exception("Some failed extraction")


args = {
    'owner': 'airflow',
}

with DAG(dag_id="censo-escolar", default_args=args, start_date=days_ago(2)) as dag:

    check_bronze_bucket = BranchPythonOperator(
        task_id="check-bronze-bucket",
        python_callable=check_files,
        provide_context=True,
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create-gke-cluster',
        project_id=PROJECT,
        location="us-central1-a",
        body=get_cluster_config(),
    )

    with TaskGroup(group_id="extract-files") as extract_files:
        folders_in_bronze_bucket = '{ ti.xcom_pull(task_ids="check-bronze-bucket", key="folders_in_bronze_bucket") }}'
        for year in YEARS:
            if year not in folders_in_bronze_bucket:
                extract_file = GKEStartPodOperator(
                    task_id=f"extract-file-{year}",
                    project_id=PROJECT,
                    location="us-central1-a",
                    cluster_name="extract-files",
                    namespace="default",
                    image=f"gcr.io/{PROJECT}/censo_escolar:latest",
                    arguments=["sh", "-c", f'python extract.py {year}'],
                    env_vars={
                        "BUCKET": BUCKET_BRONZE,
                        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/service-account.json"
                    },
                    secrets=[get_secret()],
                    name=f"extract-file-{year}",
                    on_failure_callback=extract_file_error_callback,
                    get_logs=True,
                    #is_delete_operator_pod=True,
                )

    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="delete-gke-cluster",
        name="extract-files",
        project_id=PROJECT,
        location="us-central1-a"
    )

    check_extractions = BranchPythonOperator(
        task_id="check-extractions",
        python_callable=check_extraction,
        provide_context=True
    )

    some_failed_extraction = PythonOperator(
        task_id="some-failder-extration",
        python_callable=raise_exception_operator
    )

    check_silver_bucket = DummyOperator(
        task_id="check-silver-bucket"
    )

    check_bronze_bucket >> create_gke_cluster >> extract_files
    extract_files >> destroy_gke_cluster
    extract_files >> check_extractions

    check_extractions >> some_failed_extraction
    check_extractions >> check_silver_bucket

    #check_bronze_bucket >> create_gke_cluster >> extract_files >> destroy_gke_cluster >> check_silver_bucket
    check_bronze_bucket >> create_gke_cluster >> extract_files >> check_silver_bucket
    check_bronze_bucket >> check_silver_bucket
