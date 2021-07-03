import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
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
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))


def get_cluster_def():
    cpu = {
        "resource_type": "cpu",
        "maximum": 24,
        "minimum": 1
    }
    memory = {
        "resource_type": "memory",
        "maximum": 100,
        "minimum": 1,
    }

    node_pool_config = {
        "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
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

    private_cluster_config = {
        "enable_private_nodes": True,
        "master_ipv4_cidr_block": "10.0.0.0/9"
    }

    cluster_def = {
        "name": "extraction-cluster",
        "initial_node_count": 1,
        "autoscaling": cluster_auto_scaling,
        "location": "southamerica-east1-a",
        "node_config": default_node_pool_config
    }
    return cluster_def


def check_years_not_downloaded(**context):
    ti = context["ti"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    client = storage.Client()
    bucket = client.get_bucket(DATA_LAKE)
    years_in_landing_zone = set([int(blob.name.split("/")[2])
                           for blob in list(bucket.list_blobs(prefix="landing_zone/censo-escolar"))])
    years_not_in_landing_zone = " ".join(str(year)
                                         for year in (set(YEARS) - years_in_landing_zone))
    if years_not_in_landing_zone:
        ti.xcom_push(key="years_not_in_landing_zone", value=years_not_in_landing_zone)
        return true_option
    else:
        return false_option


def check_year_downloaded(**context):
    ti = context["ti"]
    year = context["year"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    years_not_in_landing_zone = ti.xcom_pull(task_ids="check_landing_zone", key="years_not_in_landing_zone")
    if year not in years_not_in_landing_zone:
        return true_option
    else:
        return false_option


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


def raise_exception_operator():
    raise Exception("Some failed extraction")


with DAG(dag_id="censo-escolar", default_args={'owner': 'airflow'}, start_date=days_ago(0)) as dag:

    check_landing_zone = BranchPythonOperator(
        task_id="check_landing_zone",
        python_callable=check_years_not_downloaded,
        provide_context=True,
        op_kwargs={"true_option": "create_gke_cluster",
                   "false_option": "extraction_finished"}
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create_gke_cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_cluster_def()
    )

    with TaskGroup(group_id="extract_files") as extract_files:
        extract_files_tasks = []
        for year in YEARS:
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

            extraction_year_finished = DummyOperator(
                task_id=f"extraction_year_{year}_finished"
            )

            check_year = BranchPythonOperator(
                task_id=f"check_year_{year}",
                python_callable=check_year_downloaded,
                provide_context=True,
                op_kwargs={"true_option": f"extraction_year_{year}_finished",
                           "false_option": f"extract_file_{year}",
                           "year": str(year)}
            )
            extract_files_tasks.append((extract_file, extraction_year_finished, check_year))

            check_year >> extract_file >> extraction_year_finished
            check_year >> extraction_year_finished

    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy_gke_cluster",
        name="extraction-cluster",
        project_id=PROJECT,
        location="southamerica-east1-a",
        depends_on_past=False
    )

    check_extractions = BranchPythonOperator(
        task_id="check_extractions",
        python_callable=check_years_not_downloaded,
        provide_context=True,
        op_kwargs={"true_option": "some_failed_extraction",
                   "false_option": "extraction_finished"}
    )

    some_failed_extraction = PythonOperator(
        task_id="some_failed_extraction",
        python_callable=raise_exception_operator
    )

    extraction_finished = DummyOperator(
        task_id="extraction_finished"
    )

    check_landing_zone >> create_gke_cluster
    check_landing_zone >> extraction_finished

    create_gke_cluster >> extract_files >> destroy_gke_cluster
    extract_files >> check_extractions

    check_extractions >> some_failed_extraction
    check_extractions >> extraction_finished
