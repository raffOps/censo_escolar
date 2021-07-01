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
        "maximum": 25,
        "minimum": 1
    }
    memory = {
        "resource_type": "memory",
        "maximum": 50,
        "minimum": 1,
    }

    node_pool_config = {
        "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "machine_type": "e2-medium"
    }

    cluster_auto_scaling = {
        "enable_node_autoprovisioning": True,
        "resource_limits": [cpu, memory],
        "autoprovisioning_node_pool_defaults": node_pool_config
    }

    default_node_pool_config = {
        "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "machine_type": "e2-micro"
    }

    cluster_def = {
        "name": "extraction-cluster",
        "initial_node_count": 1,
        "autoscaling": cluster_auto_scaling,
        "location": "southamerica-east1-a",
        "node_config": default_node_pool_config
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


def check_extraction(**context):
    ti = context["ti"]
    client = storage.Client()
    bucket = client.get_bucket(DATA_LAKE)
    years_in_bucket = set([int(blob.name.split("/")[2])
                           for blob in list(bucket.list_blobs(prefix="landing_zone/censo-escolar"))]
                          )
    years_not_in_bucket = " ".join(str(year) for year in (YEARS - years_in_bucket))
    if years_not_in_bucket:
        ti.xcom_push(key="years_not_in_bucket_pos_extract", value=years_not_in_bucket)
        return "some_failed_extraction"
    else:
        return "check_processing_zone"


def raise_exception_operator():
    raise Exception("Some failed extraction")


with DAG(dag_id="censo-escolar", default_args={'owner': 'airflow'}) as dag:

    check_landing_zone = BranchPythonOperator(
        task_id="check_landing_zone",
        python_callable=check_files,
        provide_context=True
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create_gke_cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_cluster_def()
    )

    with TaskGroup(group_id="extract_files") as extract_files:
        years_not_in_bronze_bucket = '{{ ti.xcom_pull(task_ids="check-bronze-bucket", key="years_not_in_bucket") }}'
        for year in YEARS:
            if str(year) not in years_not_in_bronze_bucket:
                extract_file = GKEStartPodOperator(
                    task_id=f"extract_file_{year}",
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
                    name=f"extract-file-{year}",
                    get_logs=True,
                    startup_timeout_seconds=600
                )
#
    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy_gke_cluster",
        name="extraction-cluster",
        project_id=PROJECT,
        location="southamerica-east1-a"
    )

    check_extractions = BranchPythonOperator(
        task_id="check_extractions",
        python_callable=check_extraction,
        provide_context=True
    )

    some_failed_extraction = PythonOperator(
        task_id="some_failed_extraction",
        python_callable=raise_exception_operator
    )

    check_processing_zone = DummyOperator(
        task_id="check_processing_zone"
    )

    check_landing_zone >> create_gke_cluster
    check_landing_zone >> check_processing_zone

    create_gke_cluster >> extract_files >> destroy_gke_cluster
    extract_files >> check_extractions

    check_extractions >> some_failed_extraction
    check_extractions >> check_processing_zone
