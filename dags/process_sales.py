from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
import os

def run_job1(**task_context):
    hook = HttpHook(
        method='POST',
        http_conn_id='conn_1'
    )
    raw_dir = os.path.join(os.pardir, 'raw/sales')
    ds = task_context.get("ds")
    response = hook.run('/', json={
        "date": ds ,
        "raw_dir": f"{raw_dir}/{ds}/"
    })
    assert response.status_code == 201, response.text

def run_job2(**task_context):
    hook = HttpHook(
        method='POST',
        http_conn_id='conn_2'
    )
    ds = task_context.get("ds")
    raw_dir = os.path.join(os.pardir, 'raw/sales')
    stg_dir = os.path.join(os.pardir, 'stg/sales')
    response = hook.run('/', json={
        "stg_dir": f"{stg_dir}/{ds}/",
        "raw_dir": f"{raw_dir}/{ds}/"
    })
    assert response.status_code == 201, response.text

dag = DAG(
    dag_id="process_sales",
    schedule="0 1 * * *",
    start_date=datetime.strptime("2022-08-09", "%Y-%m-%d"),
    end_date=datetime.strptime("2022-08-12", "%Y-%m-%d"),
    catchup=True,
    max_active_runs=1
)

task1 = PythonOperator(
    task_id="extract_data_from_api",
    dag=dag,
    python_callable=run_job1

)

task2 = PythonOperator(
    task_id="convert_to_avro",
    dag=dag,
    python_callable=run_job2
)

task1 >> task2
