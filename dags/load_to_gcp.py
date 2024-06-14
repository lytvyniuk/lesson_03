from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.dummy import DummyOperator

# Constants
BUCKET_NAME = 'sales_data_1'
LOCAL_FILE_PATH = '/opt/airflow/dags/data/sales_{{ ds }}.csv'
DESTINATION_FILE_PATH = 'src1/sales/{{ ds | replace("-", "/") }}/sales_{{ ds }}.csv'

default_args = {
    'owner': 'airflow'
}

with DAG(
        'upload_csv_to_gcs',
        default_args=default_args,
        description='A simple DAG to upload files to GCS',
        schedule="0 1 * * *",
        start_date=datetime.strptime("2022-08-09", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-08-11", "%Y-%m-%d"),
        catchup=True,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(
        task_id='start'
    )

    # Task to upload a file to GCS
    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_gcs',
        src=LOCAL_FILE_PATH,
        dst=DESTINATION_FILE_PATH,
        bucket=BUCKET_NAME,
        mime_type='text/plain'
    )

    end_task = DummyOperator(
        task_id='end'
    )

    start_task >> upload_file_to_gcs >> end_task