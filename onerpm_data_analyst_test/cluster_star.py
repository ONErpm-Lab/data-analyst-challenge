from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.models import Variable
from helpers import DAG_DEFAULT_ARGS, get_other_cluster_id_by_name
import datetime

REGION = Variable.get("REGION")
SUBNET_ID = Variable.get("SUBNET_ID")
EMR_LOG_URI = Variable.get("EMR_LOG_URI")
AWS_ID = Variable.get("AWS_ID")
EMR_ID = Variable.get("EMR_ID")
CLUSTER_NAME = Variable.get("EMR_CLUSTER_NAME")
S3_BASE_URL = Variable.get('S3_BASE_URL')
JOB_FLOW_ROLE = Variable.get('JOB_FLOW_ROLE')
SERVICE_ROLE = Variable.get('SERVICE_ROLE')
S3_LAYERS_URL = Variable.get('S3_LAYERS_URL')
SCHEDULE_INTERVAL = Variable.get("EMR_CLUSTER_START_SCHEDULE")

JOB_FLOW_OVERRIDES = {
    "Name": "emr_cluster_datalake",
    "ReleaseLabel": "emr-6.9.0",
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Livy"},
        {"Name": "JupyterEnterpriseGateway"}
    ],
    "StepConcurrencyLevel": 3,
    "AutoTerminationPolicy": {"IdleTimeout": 2400},
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}
                }
            ],
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "spark.submit.pyFiles": ""  # Evita que los .jar se incluyan incorrectamente
            }
        },
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
            }
        }
    ],
    "Instances": {
        "Ec2SubnetId": SUBNET_ID,
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m6a.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "c6gd.2xlarge",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "BootstrapActions": [
        {
            "Name": "Install Dependencies",
            "ScriptBootstrapAction": {
                "Path": S3_BASE_URL + "DAGs_requirements/install_dependencies.sh",
                "Args": [
                    "pandas",
                    "psycopg2-binary",
                    "sqlalchemy",
                    "numpy",
                    "s3fs",
                    "fsspec",
                    "pyarrow",
                    "fastapi",
                    "fastparquet",
                    "boto3",
                    "urllib3<2.0",
                    "requests<2.29"
                ],
            },
        },
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
    "LogUri": EMR_LOG_URI,
    "Tags": [
        {
            "Key": "CentroCusto",
            "Value": "05.07.01-3"
        },
        {
            "Key": "Departamento",
            "Value": "BI"
        },
        {
            "Key": "Owner",
            "Value": "BI"
        },
        {
            "Key": "Area",
            "Value": "BI"
        },
        {
            "Key": "IAC",
            "Value": "False"
        }
    ]
}

class CustomEmrJobFlowSensor(EmrJobFlowSensor):
    NON_TERMINAL_STATES = ["STARTING", "BOOTSTRAPPING", "TERMINATING"]

def check_other_cluster_exists():
    cluster_id = get_other_cluster_id_by_name()
    if cluster_id is None or cluster_id == "":
        return "end_workflow"
    else:
        return "get_other_cluster"

with DAG(
    dag_id="cluster_start",
    description="Start EMR cluster to process data in Datalake",
    start_date=datetime.datetime(2023, 4, 20),
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["cluster"],
    default_args=DAG_DEFAULT_ARGS
) as dag:
    
    begin = DummyOperator(task_id="begin_workflow")

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default"
    )

    cluster_sensor = CustomEmrJobFlowSensor(
        task_id="cluster_sensor",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_ID,
        target_states=["WAITING"],
        poke_interval=30
    )

    other_cluster_exists = BranchPythonOperator(
        task_id="other_cluster_exists",
        python_callable=check_other_cluster_exists,
        provide_context=True,
        dag=dag,
    )

    get_other_cluster = PythonOperator(
        task_id="get_other_cluster",
        python_callable=get_other_cluster_id_by_name,
        provide_context=True,
        dag=dag,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='get_other_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )

    end = DummyOperator(task_id="end_workflow")

    begin >> create_emr_cluster >> cluster_sensor >> other_cluster_exists >> get_other_cluster >> terminate_emr_cluster >> end
    begin >> create_emr_cluster >> cluster_sensor >> other_cluster_exists >> end