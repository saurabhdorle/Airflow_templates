import datetime
from datetime import date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

DAG_NAME = "GCS_TO_BQ"
BUCKET_NAME = "{BUCKET_NAME}"
TABLE_NAME = "{TABLE_ID}"
FILE_PATH = "{FILE_PATH}" # File path excluding bucket name

SCHEMA = [  {'name': 'col1_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'col2_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'col3_name', 'type': 'STRING', 'mode': 'NULLABLE'}
         ]

default_args = {
    "start_date": datetime.datetime(2023,1 , 1),
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval= None, # "0 0 * * *" in this format can set schedule as required
    max_active_runs=1,
    catchup=False,
) as dag:
    
    # Dummy Start node which has no functionality
    start = DummyOperator(task_id="start")

    # GCS to BQ operator
    bq_to_gcs_test = GoogleCloudStorageToBigQueryOperator(
            task_id = 'gcs_to_bq',
            bucket = BUCKET_NAME,
            source_objects = [f"{FILE_PATH}.csv"], # [f"{FILE_PATH}/*.csv"] if multiple files are there
            source_format = 'CSV',
            destination_project_dataset_table = TABLE_NAME,
            skip_leading_rows = 1,
            schema_fields = SCHEMA,
            autodetect = False, # True if schema is not defined manually
            write_disposition = 'WRITE_TRUNCATE', # WRITE_APPEND
            create_disposition = 'CREATE_IF_NEEDED',
            allow_jagged_rows = True, # allows for missing values
        )

    end = DummyOperator(task_id="end")


start >> bq_to_gcs_test >> end
