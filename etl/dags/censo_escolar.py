import json
from math import ceil
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
    GKECreateClusterOperator,
    GKEDeleteClusterOperator
)
from kubernetes.client import V1ResourceRequirements
from google.cloud import storage

DATA_LAKE = Variable.get("DATA_LAKE")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))


def calculate_cluster_size(amount_years):
    return ceil(int(amount_years)/2) + 1


def get_cluster_def():
    default_node_pool_config = {
        "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "machine_type": "e2-standard-4"
    }

    cluster_def = {
        "name": "extraction",
        "initial_node_count": '{{ ti.xcom_pull(task_ids="check_landing_zone", key="cluster_size") }}',
        "location": "southamerica-east1-a",
        "node_config": default_node_pool_config,
    }
    return cluster_def


# def get_secret():
#     return Secret(
#         deploy_type='volume',
#         deploy_target='/var/secrets/google',
#         secret='gcs-credentials',
#         key='key.json')


def check_years_not_downloaded(**context):
    ti = context["ti"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    client = storage.Client()
    bucket = client.get_bucket(DATA_LAKE)
    years_in_landing_zone = set([int(blob.name.split("/")[2])
                             for blob in list(bucket.list_blobs(prefix="landing_zone/censo-escolar"))])
    years_not_in_landing_zone = set(YEARS) - years_in_landing_zone
    if years_not_in_landing_zone:
        ti.xcom_push(key="years_in_landing_zone", value=json.dumps(list(years_in_landing_zone)))
        ti.xcom_push(key="cluster_size", value=calculate_cluster_size(len(years_not_in_landing_zone)))
        return true_option
    else:
        return false_option


def check_year_downloaded(**context):
    ti = context["ti"]
    year = context["year"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    years_in_landing_zone = ti.xcom_pull(task_ids="check_landing_zone", key="years_in_landing_zone")
    if year in json.loads(years_in_landing_zone):
        return true_option
    else:
        return false_option


def get_pod_resources():
    return V1ResourceRequirements(
        requests={
            "cpu": "1.7",
            "memory": "4G"
        },
        limits={
            "cpu": "1.7",
            "memory": "4G"
        }
    )


with DAG(dag_id="censo-escolar", default_args={'owner': 'airflow'}, start_date=days_ago(0)) as dag:
    check_landing_zone = BranchPythonOperator(
        task_id="check_landing_zone",
        python_callable=check_years_not_downloaded,
        provide_context=True,
        op_kwargs={"true_option": 'create_gke_cluster',
                   "false_option": "extraction_finished_with_sucess"}
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create_gke_cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_cluster_def()
    )

    with TaskGroup(group_id="extract_files") as extract_files:
        for year in YEARS:
            check_year = BranchPythonOperator(
                task_id=f"check_year_{year}",
                python_callable=check_year_downloaded,
                provide_context=True,
                op_kwargs={"true_option": f"extract_files.extraction_year_{year}_finished",
                           "false_option": f"extract_files.extract_file_{year}",
                           "year": year}
            )

            extract_file = GKEStartPodOperator(
                task_id=f"extract_file_{year}",
                project_id=PROJECT,
                location="southamerica-east1-a",
                cluster_name="extraction",
                namespace="default",
                image=f"gcr.io/{PROJECT}/censo_escolar:latest",
                arguments=["sh", "-c", f'python extract.py {year}'],
                env_vars={
                    "DATA_LAKE": DATA_LAKE,
                    'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/key.json'
                },
                resources=get_pod_resources(),
                name=f"extract-file-{year}",
                #is_delete_operator_pod=True,
                get_logs=True,
                startup_timeout_seconds=600,
                #secrets=[get_secret()]
            )

            extraction_year_finished = DummyOperator(
                task_id=f"extraction_year_{year}_finished",
                trigger_rule="all_success"
            )

            check_year >> extract_file >> extraction_year_finished
            check_year >> extraction_year_finished

    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy_gke_cluster",
        name="extraction-cluster",
        project_id=PROJECT,
        location="southamerica-east1-a",
        trigger_rule="all_done",
        depends_on_past=True
    )

    extraction_finished_with_sucess = DummyOperator(
        task_id="extraction_finished_with_sucess",
        trigger_rule='none_failed'
    )

    check_landing_zone >> [create_gke_cluster, extraction_finished_with_sucess]

    create_gke_cluster >> extract_files >> [destroy_gke_cluster, extraction_finished_with_sucess]