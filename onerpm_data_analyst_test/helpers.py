import boto3, datetime, pytz
from airflow.models import Variable

# =============== VARIABLES =====================================================================
REGION = Variable.get("REGION")
SUBNET_ID = Variable.get("SUBNET_ID")
EMR_LOG_URI = Variable.get("EMR_LOG_URI")
AWS_ID = "aws_default"
EMR_ID = "emr_default"
CLUSTER_NAME = "emr_cluster_datalake"
S3_BASE_URL = Variable.get('S3_BASE_URL')
JOB_FLOW_ROLE = Variable.get('JOB_FLOW_ROLE')
SERVICE_ROLE = Variable.get('SERVICE_ROLE')
S3_LAYERS_URL = Variable.get('S3_LAYERS_URL')
# ================================================================================================

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 4, 20,tzinfo=pytz.timezone('America/Sao_Paulo')), # Start the DAG at 7 AM
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}

def get_cluster_id_by_name():
    emr_client = boto3.client('emr', region_name=REGION)
    response = emr_client.list_clusters()
    
    clusters = response['Clusters']
    clusters = sorted(clusters, key=lambda x: x["Status"]["Timeline"]["CreationDateTime"], reverse=True)
    for cluster in clusters:
        # check if cluster has a availble state:
        if not cluster['Status']['State'].upper() in ['RUNNING', 'WAITING']:
            continue
        if cluster['Name'] == CLUSTER_NAME:
            return cluster['Id']
    
    return None

def get_other_cluster_id_by_name():
    """ check if exists other cluster with the same name active, to terminate """
    emr_client = boto3.client('emr', region_name=REGION)
    response = emr_client.list_clusters()

    found_active = False
    
    clusters = response['Clusters']
    clusters = sorted(clusters, key=lambda x: x["Status"]["Timeline"]["CreationDateTime"], reverse=True)
    for cluster in clusters:
        # check if cluster has a availble state:
        if not cluster['Status']['State'].upper() in ['RUNNING', 'WAITING']:
            continue
        if cluster['Name'] == CLUSTER_NAME:
            if found_active:
                return cluster['Id']
            else:
                found_active = True
    
    return None