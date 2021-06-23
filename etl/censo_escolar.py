from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.models import Variable

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


BUCKET_BRONZE = Variable.get("BUCKET_BRONZE")
BUCKET_SILVER = Variable.get("BUCKET_SILVER")
BUCKET_GOLD = Variable.get("BUCKET_GOLD")
PROJECT = Variable.get("PROJECT")
FIRST_YEAR = int(Variable.get("FIRST_YEAR"))
LAST_YEAR = int(Variable.get("LAST_YEAR"))
YEARS = list(range(FIRST_YEAR, LAST_YEAR+1))


def get_cluster_extract():
    cpu = ResourceLimit(resource_type="cpu", maximum=4, minimum=1)
    memory = ResourceLimit(resource_type="memory", maximum=8, minimum=1)

    cluster_auto_scaling = ClusterAutoscaling(
        enable_node_autoprovisioning=True,
        resource_limits=[cpu, memory]
    )

    vertical_pod_autoscaling = VerticalPodAutoscaling(enabled=True)

    cluster_extract = Cluster(
        name="extract-files",
        initial_node_count=1,
        autoscaling=cluster_auto_scaling,
        vertical_pod_autoscaling=vertical_pod_autoscaling,
        location="us-central1-a",
        node_config=NodeConfig(oauth_scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
    )

    return cluster_extract


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
    body=get_cluster_extract(),
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

