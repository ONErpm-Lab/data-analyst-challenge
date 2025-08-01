from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from helpers import get_cluster_id_by_name, S3_LAYERS_URL, REGION, DAG_DEFAULT_ARGS


SPARK_STEPS = [
    {
        'Name': 'silver_to_gold_bi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--py-files', ','.join([
                    f'{S3_LAYERS_URL}code/__init__.py',
                    f'{S3_LAYERS_URL}code/layers_helpers.py',
                ]),
                f'{S3_LAYERS_URL}code/sz_gz_process/bi/read_report.py',
                '--s3path', S3_LAYERS_URL, '--s3region', REGION
            ],
        }
    }
]


with DAG(
    dag_id='silver_to_gold_bi',
    description='Process CUSTOMER/PROJECT data to find customer current state',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval= None,
    catchup=False,
    tags=['sz_gz']
) as dag:

    begin = DummyOperator(
        task_id='begin_workflow'
    )


    get_cluster = PythonOperator(
        task_id="get_cluster",
        python_callable=get_cluster_id_by_name,
        provide_context=True,
        dag=dag
    )

    JOB_FLOW_ID = "{{ task_instance.xcom_pull(task_ids='get_cluster', key='return_value') }}"

    add_step = EmrAddStepsOperator(
        task_id='submit_spark_application',
        job_flow_id=JOB_FLOW_ID,
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    check_step = EmrStepSensor(
        task_id='check_submission_status',
        job_flow_id=JOB_FLOW_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_application', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        poke_interval=10
    )

    end = DummyOperator(
        task_id='end_workflow'
    )

    begin >> get_cluster >> add_step >> check_step >> end






