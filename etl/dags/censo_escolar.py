import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.kubernetes.secret import Secret
from kubernetes.client import V1ResourceRequirements

from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator
)

from google.cloud import storage

DATA_LAKE = Variable.get("DATA_LAKE")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = set(range(FIRST_YEAR, LAST_YEAR + 1))


def get_cluster_def():
    cpu = {
        "resource_type": "cpu",
        "maximum": 20,
        "minimum": 1
    }
    memory = {
        "resource_type": "memory",
        "maximum": 32,
        "minimum": 4,
    }

    node_config = {
        "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
       # "management": {"auto_repair": False}
    }

    cluster_auto_scaling = {
        "enable_node_autoprovisioning": True,
        "resource_limits": [cpu, memory],
        "autoprovisioning_node_pool_defaults": node_config
    }

    cluster_def = {
        "name": "extraction-pool",
        "initial_node_count": 1,
        "autoscaling": cluster_auto_scaling,
        "location": "southamerica-east1-a",
        "node_config": node_config
    }
    return cluster_def


def check_files(**context):
    ti = context["ti"]
    client = storage.Client()
    bucket = client.get_bucket(DATA_LAKE)
    years_in_bucket = set([int(blob.name.split("/")[2])
                           for blob in list(bucket.list_blobs(prefix="landing_zone/censo-escolar"))]
                          )
    years_not_in_bucket = " ".join(str(year) for year in (YEARS - years_in_bucket))
    if years_not_in_bucket:
        ti.xcom_push(key="years_not_in_bucket", value=years_not_in_bucket)
        return "create-gke-cluster"
    else:
        return "check-silver-bucket"


def get_pod_resources():
    return V1ResourceRequirements(
        requests={
            "cpu": "1",
            "memory": "2G"
        },
        limits={
            "cpu": "1",
            "memory": "2G"
        }
    )
#
#
# def extract_file_error_callback(**context):
#     instance = context["task_instance"]
#     print(f"Error extraction: {instance}")
#     ti = context["ti"]
#     ti.xcom_push(key="failed_extraction", value=True)
#
#
# def check_extraction(**context):
#     ti = context["ti"]
#     any_failure = ti.xcom_pull(key="failed extraction")
#     if any_failure:
#         return "some-failed-extraction"
#     else:
#         return "check-silver-bucket"
#
#
# def raise_exception_operator():
#     raise Exception("Some failed extraction")
#
#
args = {
    'owner': 'airflow',
}

with DAG(dag_id="censo-escolar", default_args=args, start_date=days_ago(2)) as dag:

    # check_bronze_bucket = BranchPythonOperator(
    #     task_id="check-bronze-bucket",
    #     python_callable=check_files,
    #     provide_context=True
    # )
    #
    # create_gke_cluster = GKECreateClusterOperator(
    #     task_id='create-gke-cluster',
    #     project_id=PROJECT,
    #     location="southamerica-east1-a",
    #     body=get_cluster_def()
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
                        "DATA_LAKE": DATA_LAKE,
                        "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"
                    },
                    resources=get_pod_resources(),
                #    secrets=[get_secret()],
                    name=f"extract-file-{year}",
                    #on_failure_callback=extract_file_error_callback,
                    get_logs=True,
                    startup_timeout_seconds=600
                    #is_delete_operator_pod=True,
                )
#
#     destroy_gke_cluster = GKEDeleteClusterOperator(
#         task_id="destroy-gke-cluster",
#         name="extraction-cluster",
#         project_id=PROJECT,
#         location="southamerica-east1-a"
#     )
#     # #
#     # check_extractions = BranchPythonOperator(
#     #     task_id="check-extractions",
#     #     python_callable=check_extraction,
#     #     provide_context=True
#     # )
#     #
#     # some_failed_extraction = PythonOperator(
#     #     task_id="some-failer-extration",
#     #     python_callable=raise_exception_operator
#     # )
#     #
#     # check_silver_bucket = DummyOperator(
#     #     task_id="check-silver-bucket"
#     # )
#
    extract_files
    #check_bronze_bucket >> create_gke_cluster #>> extract_files >> destroy_gke_cluster
#     # check_extractions >> some_failed_extraction
#     # check_extractions >> check_silver_bucket
#
#     # check_bronze_bucket >> check_silver_bucket
#     #
#     # extract_files >> check_extractions
#     # check_extractions >> some_failed_extraction
#     # check_extractions >> check_silver_bucket
#
