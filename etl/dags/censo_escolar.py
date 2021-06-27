import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.kubernetes.secret import Secret
from kubernetes.client import V1ResourceRequirements
from google.cloud.container_v1.types import (
    Cluster,
    ClusterAutoscaling,
    VerticalPodAutoscaling,
    ResourceLimit,
    NodeConfig
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator
)

from google.cloud import storage

#if Variable.get("PROJECT"):

BUCKET_BRONZE = Variable.get("BUCKET_BRONZE")
BUCKET_SILVER = Variable.get("BUCKET_SILVER")
BUCKET_GOLD = Variable.get("BUCKET_GOLD")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = set(range(FIRST_YEAR, LAST_YEAR + 1))


def get_cluster_config():
    cpu = ResourceLimit(resource_type="cpu", maximum=13, minimum=1)
    memory = ResourceLimit(resource_type="memory", maximum=30, minimum=4)

    cluster_auto_scaling = ClusterAutoscaling(
        enable_node_autoprovisioning=True,
        resource_limits=[cpu, memory]
    )

    vertical_pod_autoscaling = VerticalPodAutoscaling(enabled=True)
    years_not_in_bronze_bucket = '{{ ti.xcom_pull(task_ids="check-bronze-bucket", key="years_not_in_bucket") }}'
    nodes = int(len(years_not_in_bronze_bucket.split()) / 2) + 3

    cluster_config = Cluster(
        name="extraction-cluster",
        initial_node_count=nodes,
        autoscaling=cluster_auto_scaling,
        vertical_pod_autoscaling=vertical_pod_autoscaling,
        location="southamerica-east1-a",
        node_config=NodeConfig(oauth_scopes=["https://www.googleapis.com/auth/cloud-platform"])
    )

    return cluster_config


def get_pod_resources():
    return V1ResourceRequirements(
        requests={
            "storage": "40G",
            "cpu": "1",
            "memory": "2G"
        }
    )

# def get_create_secret_cmd():
#     return f"gcloud container clusters get-credentials extraction-cluster --zone us-central1-a --project {PROJECT} && " + \
#            f"gcloud iam service-accounts keys create key.json --iam-account=etl-service-account@{PROJECT}.iam.gserviceaccount.com && " + \
#            f"kubectl create secret generic gcs-credentials --from-file key.json"
#
#
# def get_secret():
#     secret = Secret(
#         deploy_type='volume',
#         deploy_target='/var/secrets/google',
#         secret='gcs-credentials',
#         key='key.json')
#     return secret


def check_files(**context):
    ti = context["ti"]
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_BRONZE)
    years_in_bucket = set([int(blob.name.split("/")[1])
                           for blob in list(bucket.list_blobs(prefix="censo-escolar"))]
                          )
    years_not_in_bucket = " ".join(str(year) for year in (YEARS - years_in_bucket))
    if years_not_in_bucket:
        ti.xcom_push(key="years_not_in_bucket", value=years_not_in_bucket)
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
        provide_context=True
    )
    #
    create_gke_cluster = GKECreateClusterOperator(
        task_id='create-gke-cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_cluster_config()
    )

    # create_cluster_secret = BashOperator(
    #     task_id="create-cluster-secret",
    #     bash_command=get_create_secret_cmd()
    # )

    with TaskGroup(group_id="extract-files") as extract_files:
        years_not_in_bronze_bucket = '{{ ti.xcom_pull(task_ids="check-bronze-bucket", key="years_not_in_bucket") }}'
        for year in YEARS:
            if str(year) not in years_not_in_bronze_bucket:
                extract_file = GKEStartPodOperator(
                    task_id=f"extract-file-{year}",
                    project_id=PROJECT,
                    location="southamerica-east1-a",
                    cluster_name="extraction-cluster",
                    namespace="default",
                    image=f"gcr.io/{PROJECT}/censo_escolar:latest",
                    arguments=["sh", "-c", f'python extract.py {year}'],
                    env_vars={
                        "BUCKET": BUCKET_BRONZE,
                        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"
                    },
                    resources=get_pod_resources(),
                #    secrets=[get_secret()],
                    name=f"extract-file-{year}",
                    on_failure_callback=extract_file_error_callback,
                    get_logs=True
                    #is_delete_operator_pod=True,
                )
    
    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy-gke-cluster",
        name="extraction-cluster",
        project_id=PROJECT,
        location="southamerica-east1-a"
    )
    #
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

    check_bronze_bucket >> create_gke_cluster >> extract_files >> destroy_gke_cluster
    extract_files >> check_extractions
    check_extractions >> some_failed_extraction
    check_extractions >> check_silver_bucket

    check_bronze_bucket >> check_silver_bucket
    #
    # extract_files >> check_extractions
    # check_extractions >> some_failed_extraction
    # check_extractions >> check_silver_bucket

