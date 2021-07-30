import json
from math import ceil
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    #DataprocCreateClusterOperator
)
from kubernetes.client import V1ResourceRequirements
from google.cloud import storage

PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("CENSO_ESCOLAR_FIRST_YEAR"))
LAST_YEAR = int(Variable.get("CENSO_ESCOLAR_LAST_YEAR"))

LANDING_BUCKET = f"{PROJECT}-landing"
PROCESSING_BUCKET = f"{PROJECT}-processing"
CONSUMER_BUCKET = f"{PROJECT}-consumer"
SCRIPTS_BUCKET = f"{PROJECT}-scripts"
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))



def calculate_cluster_size(amount_years):
    return ceil(int(amount_years)/2) + 1


def get_gke_cluster_def():
    cluster_def = {
        "name": "censo-escolar-extraction",
        "initial_node_count": '{{ ti.xcom_pull(task_ids="check_landing_zone", key="cluster_size") }}',
        "location": "southamerica-east1-a",
        "node_config": {
            "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "machine_type": "e2-standard-4"
        },
    }
    return cluster_def


def get_dataproc_cluster_def():
    cluster_def = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-4",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        },
        "worker_config": {
            "num_instances": 5,
            "machine_type_uri": "n1-standard-4",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        },
    }
    return cluster_def


def check_files(**context):
    ti = context["ti"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    client = storage.Client()
    bucket = client.get_bucket(context["zone"])
    years_in_this_zone = set([int(blob.name.split("/")[2])
                             for blob in list(bucket.list_blobs(prefix="censo-escolar"))])
    years_not_in_this_zone = set(YEARS) - years_in_this_zone
    if years_not_in_this_zone:
        ti.xcom_push(key="years", value=json.dumps(list(years_in_this_zone)))
        ti.xcom_push(key="cluster_size", value=calculate_cluster_size(len(years_not_in_this_zone)))
        return true_option
    else:
        return false_option


def check_year_downloaded(**context):
    ti = context["ti"]
    year = context["year"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    years_in_landing_zone = ti.xcom_pull(task_ids="check_landing_zone", key="years")
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
        python_callable=check_files,
        provide_context=True,
        op_kwargs={"true_option": 'create_gke_cluster',
                   "false_option": "extraction_finished_with_sucess",
                   "zone": LANDING_BUCKET}
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create_gke_cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_gke_cluster_def()
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
                cluster_name="censo-escolar-extraction",
                namespace="default",
                image=f"gcr.io/{PROJECT}/censo_escolar_extraction:latest",
                arguments=["sh", "-c", f'python extract.py {year}'],
                env_vars={
                    "BUCKET": LANDING_BUCKET,
                },
                resources=get_pod_resources(),
                name=f"extract-file-{year}",
                get_logs=True,
                startup_timeout_seconds=600
            )

            extraction_year_finished = DummyOperator(
                task_id=f"extraction_year_{year}_finished",
                trigger_rule="all_success"
            )

            check_year >> extract_file >> extraction_year_finished
            check_year >> extraction_year_finished

    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy_gke_cluster",
        name="censo-escolar-extraction",
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


    # check_processing_zone = BranchPythonOperator(
    #     task_id="check_processing_zone",
    #     python_callable=check_files,
    #     provide_context=True,
    #     op_kwargs={"true_option": 'create_dataproc_cluster',
    #                "false_option": "transformation_finished_with_sucess",
    #                "zone": "processing_zone"}
    # )

    # create_cluster = DataprocCreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT,
    #     cluster_config=get_dataproc_cluster_def(),
    #     region="us-central1-a",
    #     cluster_name="censo-escolar",
    # )
