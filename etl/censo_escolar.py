from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.models import Variable

from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator
)


BUCKET_BRONZE = Variable.make_request("BUCKET_BRONZE")
BUCKET_SILVER = Variable.make_request("BUCKET_SILVER")
BUCKET_GOLD = Variable.make_request("BUCKET_GOLD")
PROJECT = Variable.make_request("PROJECT")
FIRST_YEAR = int(Variable.make_request("FIRST_YEAR"))
LAST_YEAR = int(Variable.make_request("LAST_YEAR"))
YEARS = list(range(FIRST_YEAR, LAST_YEAR+1))


def get_cluster_config():
    cpu = {
        "resourceType": "cpu",
        "minimum": "4",
        "maximum": "1"
    }
    memory = {
        "resourceType": "memory",
        "minimum": "4",
        "maximum": "16"
    }

    cluster_auto_scaling = {
        "resourceLimits": [cpu, memory],
        "enableNodeAutoprovisioning": True
    }
    vertical_pod_autoscaling = {"enabled": True}

    node_config = {"oauthScopes": ["https://www.googleapis.com/auth/cloud-platform"]}

    cluster_config = {
        "name": "extract-cluster",
        "initialNodeCount": 3,
        "autoscaling": cluster_auto_scaling,
        "verticalPodAutoscaling": vertical_pod_autoscaling,
        "location": "us-central1-a",
        "nodeConfig": node_config
    }
    return cluster_config


args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id="censo-escolar",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2)
)


create_gke_cluster = GKECreateClusterOperator(
    task_id='create-cluster',
    project_id=PROJECT,
    location="us-central1-a",
    body=get_cluster_config(),
    dag=dag
)

with TaskGroup(group_id="extract-files", dag=dag) as extract_files:
    for year in YEARS:
        extract_file = GKEStartPodOperator(
            task_id=f"extract-file-{year}",
            project_id=PROJECT,
            location="us-central1-a",
            cluster_name="extract-files",
            namespace="default",
            image=f"gcr.io/{PROJECT}/licitacoes:latest",
            arguments=["sh", "-c", f'python extract.py {year}'],
            env_vars={
                "BUCKET": BUCKET_BRONZE
            },
            name=f"extract-file-{year}"
        )

destroy_gke_cluster = GKEDeleteClusterOperator(
    task_id="delete-cluster",
    name="extract-files",
    project_id=PROJECT,
    location="us-central1-a",
    dag=dag
)

create_gke_cluster >> extract_files >> destroy_gke_cluster

