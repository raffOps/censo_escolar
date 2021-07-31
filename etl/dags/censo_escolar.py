import json
from math import ceil
import re

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
    GKECreateClusterOperator,
    GKEDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from kubernetes.client import V1ResourceRequirements
from google.cloud import storage


PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("CENSO_ESCOLAR_FIRST_YEAR"))
LAST_YEAR = int(Variable.get("CENSO_ESCOLAR_LAST_YEAR"))

landing_bucket = f"{PROJECT}-landing"
processing_bucket = f"{PROJECT}-processing"
consumer_bucket = f"{PROJECT}-consumer"
scripts_buckets = f"{PROJECT}-scripts"
years = list(range(FIRST_YEAR, LAST_YEAR + 1))



def calculate_cluster_size(amount_years):
    return ceil(int(amount_years)/2) + 1


def get_gke_cluster_def():
    cluster_def = {
        "name": "censo-escolar-extraction",
        "initial_node_count": '{{ ti.xcom_pull(task_ids="check_landing_bucket", key="cluster_size") }}',
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
            "machine_type_uri": "n1-highmem-8",
            "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 1024},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-highmem-4",
            "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 1024}
        },
        "softaware_config": {
            "image_version": "2.0-debian10"
        },
        "gce_cluster_config": {
            "service_account": f"etl-service-account@{PROJECT}.iam.gserviceaccount.com",
            "service_account_scopes": ["cloud-platform"]
        }
    }
    return cluster_def

def get_pyspark_job_def(year):
    pyspark_job_def = {
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": "censo-escolar-transform"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{PROJECT}-scripts/censo_escolar/transformation/transform.py",
            "args": [PROJECT, year]
        }
    }


def check_years(**context):
    ti = context["ti"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    client = storage.Client()
    bucket = client.get_bucket(context["bucket"])
    years_in_this_zone = set([int(re.findall("([0-9]{4})\/", blob.name)[0])
                             for blob in list(bucket.list_blobs(prefix="censo-escolar"))
                              if re.findall("([0-9]{4})\/", blob.name)])
    years_not_in_this_zone = set(context["years"]) - years_in_this_zone
    if years_not_in_this_zone:
        ti.xcom_push(key="years", value=json.dumps(list(years_in_this_zone)))
        ti.xcom_push(key="cluster_size", value=calculate_cluster_size(len(years_not_in_this_zone)))
        return true_option
    else:
        return false_option


def check_year(**context):
    ti = context["ti"]
    year = context["year"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    years_in_landing_zone = ti.xcom_pull(task_ids="check_landing_bucket", key="years")
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
    check_landing_bucket = BranchPythonOperator(
        task_id="check_landing_bucket",
        python_callable=check_years,
        provide_context=True,
        op_kwargs={"true_option": 'create_gke_cluster',
                   "false_option": "extraction_finished_with_sucess",
                   "bucket": f"{PROJECT}-landing",
                   "years": years}
    )

    create_gke_cluster = GKECreateClusterOperator(
        task_id='create_gke_cluster',
        project_id=PROJECT,
        location="southamerica-east1-a",
        body=get_gke_cluster_def()
    )

    with TaskGroup(group_id="extract_years") as extract_years:
        for year in years:
            check_extract_year = BranchPythonOperator(
                task_id=f"check_extract_year_{year}",
                python_callable=check_year,
                provide_context=True,
                op_kwargs={"true_option": f"extract_years.extraction_year_{year}_finished",
                           "false_option": f"extract_years.extract_year_{year}",
                           "year": year}
            )

            extract_year = GKEStartPodOperator(
                task_id=f"extract_year_{year}",
                project_id=PROJECT,
                location="southamerica-east1-a",
                cluster_name="censo-escolar-extraction",
                namespace="default",
                image=f"gcr.io/{PROJECT}/censo_escolar_extraction:latest",
                arguments=["sh", "-c", f'python extract.py {year} {PROJECT}'],
                resources=get_pod_resources(),
                name=f"extract-file-{year}",
                get_logs=True,
                startup_timeout_seconds=600
            )

            extraction_year_finished = DummyOperator(
                task_id=f"extraction_year_{year}_finished",
                trigger_rule="all_success"
            )

            check_extract_year >> extract_year >> extraction_year_finished
            check_extract_year >> extraction_year_finished

    destroy_gke_cluster = GKEDeleteClusterOperator(
        task_id="destroy_gke_cluster",
        name="censo-escolar-extraction",
        project_id=PROJECT,
        location="southamerica-east1-a",
        trigger_rule="all_done"
    )

    extraction_finished_with_sucess = DummyOperator(
        task_id="extraction_finished_with_sucess",
        trigger_rule='none_failed'
    )

    check_landing_bucket >> [create_gke_cluster, extraction_finished_with_sucess]
    create_gke_cluster >> extract_years >> [destroy_gke_cluster, extraction_finished_with_sucess]


    check_processing_bucket = BranchPythonOperator(
        task_id="check_processing_bucket",
        python_callable=check_years,
        provide_context=True,
        op_kwargs={"true_option": 'create_dataproc_cluster',
                   "false_option": "transformation_finished_with_sucess",
                   "bucket": processing_bucket},
        trigger_rule="none_failed"
    )

    # create_dataproc_cluster = DataprocCreateClusterOperator(
    #     task_id="create_dataproc_cluster",
    #     project_id=PROJECT,
    #     cluster_config=get_dataproc_cluster_def(),
    #     region="us-east1-b",
    #     cluster_name="censo-escolar-transform",
    # )

    # with TaskGroup(group_id="transform_years") as transform_years:
    #     for year in years:
    #         check_transform_year = BranchPythonOperator(
    #             task_id=f"check_transform_year_{year}",
    #             python_callable=check_year,
    #             provide_context=True,
    #             op_kwargs={"true_option": f"transform_years.transform_year_{year}_finished",
    #                        "false_option": f"transform_years.transform_year_{year}",
    #                        "year": year}
    #         )

    #         transform_year = DataprocSubmitJobOperator(
    #             task_id=f"transform_years.transform_year_{year}",
    #             job=get_pyspark_job_def(year),
    #             location="us-east1-b",
    #             project_id=PROJECT
    #         )

    #         transform_year_finished = DummyOperator(
    #             task_id=f"transform_year_{year}_finished",
    #             trigger_rule="all_success"
    #         )

    #         check_transform_year >> transform_year >> extraction_year_finished
    #         check_transform_year >> transform_year_finished

    # destroy_dataproc_cluster = DataprocDeleteClusterOperator(
    #     task_id="destroy_dataproc_cluster",
    #     project_id=PROJECT,
    #     region="us-east1-b",
    #     trigger_rule="all_done",
    #     cluster_name="censo-escolar-transform",
    # )

    # transformation_finished_with_sucess = DummyOperator(
    #     task_id="transformation_finished_with_sucess",
    #     trigger_rule='none_failed'
    # )

    destroy_gke_cluster >> check_processing_bucket
    extraction_finished_with_sucess >> check_processing_bucket

    # check_processing_bucket >> [create_dataproc_cluster, extraction_finished_with_sucess]
    # create_dataproc_cluster >> transform_years >> [destroy_dataproc_cluster, transformation_finished_with_sucess]
