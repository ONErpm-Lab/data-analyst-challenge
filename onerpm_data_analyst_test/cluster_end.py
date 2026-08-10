from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.models import Variable
from helpers import DAG_DEFAULT_ARGS, get_cluster_id_by_name
import datetime

SCHEDULE_INTERVAL = Variable.get("EMR_CLUSTER_END_SCHEDULE")

def check_if_cluster_exists():
    cluster_id = get_cluster_id_by_name()
    if cluster_id:
        return "get_cluster"
    else:
        return "end_workflow"

with DAG(
    dag_id="cluster_end",
    description="Terminate EMR cluster used to process datalake data",
    start_date=datetime.datetime(2023, 8, 4),
    schedule_interval = SCHEDULE_INTERVAL,
    default_args = DAG_DEFAULT_ARGS,
    tags=["cluster"]
) as dag:
    begin = DummyOperator(task_id="begin_workflow")

    cluster_exists = BranchPythonOperator(
        task_id="cluster_exists",
        python_callable=check_if_cluster_exists,
        provide_context=True,
        dag=dag,
    )

    get_cluster = PythonOperator(
        task_id="get_cluster",
        python_callable=get_cluster_id_by_name,
        provide_context=True,
        dag=dag,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='get_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )

    end = DummyOperator(task_id="end_workflow")

    begin >> cluster_exists >> get_cluster >> terminate_emr_cluster >> end
    begin >> cluster_exists >> end