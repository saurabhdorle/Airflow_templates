from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta


DAG_NAME = "Trigger_DAG"
GCS_BQ_DAG = "GSC_to_BQ" # Name of DAG to trigger
BQ_READ_DAG = "BQ_READ" # Name of DAG to trigger
 
default_args = {
    "owner": "Saurabh",
    "start_date": "2020-12-10",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval= None,
    max_active_runs=1,
    catchup=False,
) as dag:
    
    # Dummy Start node which has no functionality
    start = DummyOperator(task_id="start")

    trigger_task_gcs = TriggerDagRunOperator(task_id='dag_trigger_task',
                                        trigger_dag_id={GCS_BQ_DAG}, # Name of DAG to trigger
                                        execution_date= "2023-09-01", #'{{execution_date}}',
                                        dag=dag)
    
    trigger_task_bq = TriggerDagRunOperator(task_id='dag_trigger_task2',
                                    trigger_dag_id={BQ_READ_DAG}, # Name of DAG to trigger
                                    execution_date= "2023-10-01", #'{{execution_date}}',
                                    dag=dag)

    # Dummy end node which has no functionality
    end = DummyOperator(task_id="end")


start >> trigger_task_gcs >> trigger_task_bq >> end
