from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from helpers import DAG_DEFAULT_ARGS


SCHEDULE_INTERVAL = Variable.get('MASTER_PROCESS_SCHEDULE')


with DAG(

    dag_id="MASTER_PROCESS",
    description="Run near real-time data pipelines",
    default_args = DAG_DEFAULT_ARGS,
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["MasterProcess"],
    concurrency = 20,
    max_active_runs=1

) as dag:

    begin_workflow = DummyOperator(
        task_id='begin_workflow'
    )

    #---------------------------------------------------------------------------------------------------------  

    source_br_step = DummyOperator(

        task_id='source_br_step'

    )

    source_br_to_bronze_cartpanda = TriggerDagRunOperator(
        task_id='source_br_to_bronze_cartpanda',
        trigger_dag_id='source_br_to_bronze_cartpanda',
        wait_for_completion=True
    )

    source_br_to_bronze_step = [

        source_br_to_bronze_cartpanda,

    ]

    bz_sz_step = DummyOperator(

        task_id='bz_sz_step'

    )

    #---------------------------------------------------------------------------------------------------------

    bronze_to_silver = TriggerDagRunOperator(
        task_id='bronze_to_silver',
        trigger_dag_id='bronze_to_silver',
        wait_for_completion=True
    )

    bronze_to_silver_step = [

        bronze_to_silver,

    ]

    sz_gz_step = DummyOperator(

        task_id='sz_gz_step'

    )

    silver_to_gold_bi = TriggerDagRunOperator(
        task_id='silver_to_gold_bi',
        trigger_dag_id='silver_to_gold_bi',
        wait_for_completion=True
    )

    silver_to_gold_bi_step   = [

        silver_to_gold_bi,

    ]

    end_workflow = DummyOperator(

        task_id='end_workflow'

    )

    #---------------------------------------------------------------------------------------------------------

    gold_bi_to_datawerehouse = TriggerDagRunOperator(
        task_id='gold_bi_to_datawerehouse',
        trigger_dag_id='gold_bi_to_datawerehouse',
        wait_for_completion=True
    )

    gold_bi_to_datawerehouse_step   = [

        gold_bi_to_datawerehouse,

    ]

    #---------------------------------------------------------------------------------------------------------

    begin_workflow >> source_br_step >> source_br_to_bronze_step 
    
    source_br_to_bronze_step >> bronze_to_silver_step >> bz_sz_step

    bronze_to_silver_step >> silver_to_gold_bi_step >> sz_gz_step
    
    silver_to_gold_bi_step >> gold_bi_to_datawerehouse_step >> end_workflow

    
