from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from helpers import get_cluster_id_by_name, S3_LAYERS_URL, REGION, DAG_DEFAULT_ARGS


SPARK_STEPS = [
    {
        'Name': 'bronze_to_silver',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.submit.pyFiles={}'.format(','.join([
                    f'{S3_LAYERS_URL}code/__init__.py',
                    f'{S3_LAYERS_URL}code/layers_helpers.py',
                ])),
                '--jars', ','.join([
                    f's3://data-lake-prd-experience/jars/hadoop-aws-3.2.1.jar',
                    f's3://data-lake-prd-experience/jars/aws-java-sdk-bundle-1.12.262.jar',
                ]),
                f'{S3_LAYERS_URL}code/sz_gz_process/bi/bz_sz_report.py',
                '--s3path', S3_LAYERS_URL, '--s3region', REGION
            ],
        }
    }
]





with DAG(
    dag_id='bronze_to_silver',
    description='Process CUSTOMER/PROJECT data to find customer current state',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval= None,
    catchup=False,
    tags=['br_sz']
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






