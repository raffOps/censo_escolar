from datetime import datetime
import json
import re
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
    GKEDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocCreateWorkflowTemplateOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator
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
YEARS_TO_ETL = list(map(str, range(FIRST_YEAR, LAST_YEAR + 1)))


def are_all_these_years_already_in_bucket(**context):
    ti = context["ti"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    client = storage.Client()
    bucket = client.get_bucket(context["bucket"])
    years_in_this_bucket = {
        re.findall("([0-9]{4})\/", blob.name)[0]
        for blob in list(bucket.list_blobs(prefix="censo-escolar"))
        if re.findall("([0-9]{4})\/", blob.name)
    }
    if (
        years_not_in_this_bucket := set(context["years"])
        - years_in_this_bucket
    ):
        ti.xcom_push(key="years_not_in_this_bucket",
                     value=list(years_not_in_this_bucket))
        ti.xcom_push(key="cluster_size",
                     value=get_gke_cluster_size(len(years_not_in_this_bucket)))

        return true_option
    elif ti.xcom_pull(task_ids="transform.check_processing_bucket",
                      key="years_not_in_this_bucket") and context["task"].task_id == "load.check_processing_bucket":
        return true_option
    else:
        return false_option


def check_year(**context):
    ti = context["ti"]
    year = context["year"]
    true_option = context["true_option"]
    false_option = context["false_option"]
    years_not_in_this_bucket = ti.xcom_pull(task_ids=context["task"],
                                            key="years_not_in_this_bucket")
    return true_option if year in years_not_in_this_bucket else false_option


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


def get_gke_cluster_size(amount_years):
    return ceil(amount_years/2) + 1


def get_gke_cluster_def():
    return {
        "name": "censo-escolar-extract",
        "initial_node_count": '{{ ti.xcom_pull(task_ids="extract.check_landing_bucket", key="cluster_size") }}',
        "location": "southamerica-east1-a",
        "node_config": {
            "oauth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "machine_type": "e2-standard-4",
        },
    }


def get_dataproc_workflow(years):
    now = str(datetime.now().timestamp()).replace(".", "")
    dataproc_workflow_id = f"censo-escolar-transform-{now}"
    workflow = {
        "id": dataproc_workflow_id,
        "name": f"projects/{PROJECT}/regions/us-east1/workflowTemplates/censo-transform",
        "placement": {
            "managed_cluster": {
                "cluster_name": "censo-escolar-transform",
                "config": {
                    "master_config": {
                        "num_instances": 1,
                        "machine_type_uri": "n1-highmem-8"
                    },
                    "worker_config": {
                        "num_instances": 2,
                        "machine_type_uri": "n1-highmem-8"
                    },
                    "gce_cluster_config": {
                        "zone_uri": "us-east1-b"
                    }
                }
            },
        },
        "jobs": []
    }

    prev_job_id = None
    jobs = []
    for year_ in years:
        step_id = f"censo-transform-{year_}"
        job = {
            "step_id": step_id,
            "pyspark_job": {
                "main_python_file_uri": f"gs://{SCRIPTS_BUCKET}/censo_escolar/transform/transform.py",
                "args": [PROJECT, year_]
            }
        }

        if prev_job_id:
            job["prerequisite_step_ids"] = [prev_job_id]

        prev_job_id = step_id
        jobs.append(job)

    workflow["jobs"] = jobs

    return workflow


def create_dataproc_workflow_substask(**context):
    ti = context["ti"]
    years_not_int_processing_bucket = ti.xcom_pull(task_ids="transform.check_processing_bucket",
                                                    key="years_not_in_this_bucket")
    workflow = get_dataproc_workflow(years_not_int_processing_bucket)
    ti.xcom_push("dataproc_workflow_id", workflow["id"])
    create_workflow_template_substask_op = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template_subtask",
        template=workflow,
        project_id=PROJECT,
        location="us-east1",
    )
    create_workflow_template_substask_op.execute(context)


def get_file_from_gcs(file, bucket):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    return bucket.get_blob(file).download_as_text()


def get_table_resource(table, project):
    return {
        "table_reference": {
            "project_id": project,
            "dataset_id": "censo_escolar",
            "table_id": table,
        },
        "external_data_configuration": {
            "source_uris": [
                f"gs://{project}-processing/censo-escolar/{table}/*.parquet"
            ],
            "source_format": "PARQUET",
            "autodetect": True,
            "hive_partitioning_options": {
                "mode": "AUTO",
                "source_uri_prefix": f"gs://{project}-processing/censo-escolar/{table}/",
            },
        },
        "location": "us",
    }


with DAG(dag_id="censo-escolar", default_args={'owner': 'airflow'}, start_date=days_ago(0)) as dag:
    with TaskGroup(group_id="extract") as extract:
        check_landing_bucket = BranchPythonOperator(
            task_id="check_landing_bucket",
            python_callable=are_all_these_years_already_in_bucket,
            provide_context=True,
            op_kwargs={
                "true_option": 'extract.create_gke_cluster',
                "false_option": "extract.extraction_finished_wih_sucess",
                "bucket": LANDING_BUCKET,
                "years": YEARS_TO_ETL
            }
        )

        create_gke_cluster = GKECreateClusterOperator(
            task_id='create_gke_cluster',
            project_id=PROJECT,
            location="southamerica-east1-a",
            body=get_gke_cluster_def()
        )

        with TaskGroup(group_id="download") as download:
            for year in YEARS_TO_ETL:
                check_before_download = BranchPythonOperator(
                    task_id=f"check_before_download_year_{year}",
                    python_callable=check_year,
                    provide_context=True,
                    op_kwargs={
                        "true_option": f"extract.download.download_year_{year}",
                        "false_option": f"extract.download.download_year_{year}_finished",
                        "year": year,
                        "task": "extract.check_landing_bucket"
                    }
                )

                download_year = GKEStartPodOperator(
                    task_id=f"download_year_{year}",
                    project_id=PROJECT,
                    location="southamerica-east1-a",
                    cluster_name="censo-escolar-extract",
                    namespace="default",
                    image=f"gcr.io/{PROJECT}/censo_escolar_extraction:latest",
                    arguments=["sh", "-c", f'python extract.py {year} {LANDING_BUCKET}'],
                    resources=get_pod_resources(),
                    name=f"extract-file-{year}",
                    get_logs=True,
                    startup_timeout_seconds=600
                )

                download_year_finished = DummyOperator(
                    task_id=f"download_year_{year}_finished",
                    trigger_rule="all_success"
                )

                check_before_download >> download_year >> download_year_finished
                check_before_download >> download_year_finished

        destroy_gke_cluster = GKEDeleteClusterOperator(
            task_id="destroy_gke_cluster",
            name="censo-escolar-extract",
            project_id=PROJECT,
            location="southamerica-east1-a",
            trigger_rule="all_done"
        )

        extraction_finished_wih_sucess = DummyOperator(
            task_id="extraction_finished_wih_sucess",
            trigger_rule='none_failed'
        )

        check_landing_bucket >> [create_gke_cluster, extraction_finished_wih_sucess]
        create_gke_cluster >> download >> [destroy_gke_cluster, extraction_finished_wih_sucess]

    with TaskGroup(group_id="transform") as transform:
        check_processing_bucket = BranchPythonOperator(
            task_id="check_processing_bucket",
            python_callable=are_all_these_years_already_in_bucket,
            provide_context=True,
            op_kwargs={
                "true_option": "transform.create_workflow_template",
                "false_option": "transform.transformation_finished_with_sucess",
                "bucket": PROCESSING_BUCKET,
                "years": YEARS_TO_ETL
            },
            trigger_rule="none_failed"
        )

        create_workflow_template = PythonOperator(
            task_id="create_workflow_template",
            python_callable=create_dataproc_workflow_substask,
            provide_context=True
        )

        run_dataproc_job = DataprocInstantiateWorkflowTemplateOperator(
            task_id="run_dataproc_job",
            template_id='{{ ti.xcom_pull(task_ids="transform.create_workflow_template", key="dataproc_workflow_id") }}',
            project_id=PROJECT,
            region="us-east1"
        )

        transformation_finished_with_sucess = DummyOperator(
            task_id="transformation_finished_with_sucess",
            trigger_rule='none_failed'
        )

        check_processing_bucket >> [create_workflow_template, transformation_finished_with_sucess]
        create_workflow_template >> run_dataproc_job >> transformation_finished_with_sucess

    with TaskGroup(group_id="load") as load:
        check_processing_bucket = BranchPythonOperator(
            task_id="check_processing_bucket",
            python_callable=are_all_these_years_already_in_bucket,
            provide_context=True,
            op_kwargs={
                "true_option": "load.delete_old_bigquery_tables",
                "false_option": "load.loading_finished_with_sucess",
                #"false_option": "load.delete_old_bigquery_tables",
                "bucket": PROCESSING_BUCKET,
                "years": YEARS_TO_ETL
            },
            trigger_rule="none_failed"
        )

        delete_old_bigquery_tables = BigQueryInsertJobOperator(
            task_id="delete_old_bigquery_tables",
            configuration={
                "query": {
                    "query": get_file_from_gcs("censo_escolar/load/delete_old_tables.sql", SCRIPTS_BUCKET),
                    "useLegacySql": False,
                }
            },
            location="us"
        )

        with TaskGroup("create_bigquery_tables") as create_bigquery_tables:
            for table in ["matriculas", "docentes", "gestores", "escolas", "turmas"]:
                BigQueryCreateEmptyTableOperator(
                    task_id=f"create_table_{table}",
                    dataset_id='censo_escolar',
                    table_id=table,
                    project_id=PROJECT,
                    table_resource=get_table_resource(table, PROJECT)
                )

        loading_finished_with_sucess = DummyOperator(
            task_id="loading_finished_with_sucess",
            trigger_rule='none_failed'
        )

        check_processing_bucket >> [delete_old_bigquery_tables, loading_finished_with_sucess]
        delete_old_bigquery_tables >> create_bigquery_tables >> loading_finished_with_sucess

    extract >> transform >> load
