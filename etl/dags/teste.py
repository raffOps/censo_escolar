from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from google.cloud.container_v1.types import (
     AutoprovisioningNodePoolDefaults,
     NodeManagement
)

management = NodeManagement(auto_repair=False)
node_pool_nap = AutoprovisioningNodePoolDefaults(oauth_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                                                      management=management)
args = {
    'owner': 'airflow',
}

dag = DAG(dag_id="test", default_args=args, start_date=days_ago(2))
DummyOperator(task_id="dummy", dag=dag)
