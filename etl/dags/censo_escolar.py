import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from kubernetes.client import V1ResourceRequirements

from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

from google.cloud import storage

DATA_LAKE = Variable.get("DATA_LAKE")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))


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
            "cpu": "1.5",
            "memory": "5G"
        },
        limits={
            "cpu": "1.5",
            "memory": "5G"
        }
    )


with DAG(dag_id="censo-escolar", default_args={'owner': 'airflow'}, start_date=days_ago(0)) as dag:

    check_landing_zone = BranchPythonOperator(
        task_id="check_landing_zone",
        python_callable=check_years_not_downloaded,
        provide_context=True,
        op_kwargs={"true_option": "create_gke_cluster",
                   "false_option": "extraction_finished_with_sucess"}
    )

    with TaskGroup(group_id="extract_files") as extract_files:
        for year in YEARS:
            extract_file = GKEStartPodOperator(
                task_id=f"extract_file_{year}",
                project_id=PROJECT,
                location="southamerica-east1-a",
                cluster_name="extraction",
                namespace="default",
                image=f"gcr.io/{PROJECT}/censo_escolar:latest",
                arguments=["sh", "-c", f'python extract.py {year}'],
                env_vars={
                    "DATA_LAKE": DATA_LAKE
                },
                resources=get_pod_resources(),
                name=f"extract-file-{year}",
                node_selectors={"cloud.google.com/gke-nodepool": "extraction"},
                is_delete_operator_pod=True,
                get_logs=True,
                startup_timeout_seconds=600
            )

            extraction_year_finished = DummyOperator(
                task_id=f"extraction_year_{year}_finished",
                trigger_rule="all_success"
            )

            check_year = BranchPythonOperator(
                task_id=f"check_year_{year}",
                python_callable=check_year_downloaded,
                provide_context=True,
                op_kwargs={"true_option": f"extract_files.extraction_year_{year}_finished",
                           "false_option": f"extract_files.extract_file_{year}",
                           "year": year}
            )

            check_year >> extract_file >> extraction_year_finished
            check_year >> extraction_year_finished

    extraction_finished_with_sucess = DummyOperator(
        task_id="extraction_finished_with_sucess",
        trigger_rule='none_failed'
    )

    check_landing_zone >> [extract_files, extraction_finished_with_sucess]

    extract_files >> extraction_finished_with_sucess

