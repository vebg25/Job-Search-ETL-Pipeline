from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime,timedelta
import json 
import requests
import boto3
from io import StringIO
import pandas as pd

default_args = {
  "owner":"Vaibhav",
  "depends_on_past":False,
  "start_date":datetime(2025,2,15)
}

dag=DAG(
  dag_id="job_search_etl_dag",
  default_args=default_args,
  description="ETL Process for Job Data",
  schedule_interval=timedelta(days=1),
  catchup=False
)

def fetch_job_data(**kwargs):
  client_key=Variable.get("job_search_client_key")
  url = "https://jsearch.p.rapidapi.com/search"

  querystring = {"query":"developer jobs in chicago","page":"1","num_pages":"1","country":"us","date_posted":"all"}

  headers = {
    "x-rapidapi-key": client_key,
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
  }

  response = requests.get(url, headers=headers, params=querystring)

  filename="job_search_raw_"+datetime.now().strftime("%Y%m%d%H%M%S")+".json"

  kwargs['ti'].xcom_push(key="job_search_filename",value=filename)
  kwargs['ti'].xcom_push(key="job_search_data",value=json.dumps(response.json().get('data')))

fetch_data = PythonOperator(
  task_id="fetch_job_data",
  python_callable=fetch_job_data,
  dag=dag
)

store_raw_to_s3= S3CreateObjectOperator(
  task_id="upload_raw_data_to_s3",
  aws_conn_id="aws_conn",
  s3_bucket="job-search-etl-aws-dag",
  s3_key="raw_data/{{task_instance.xcom_pull(task_ids='fetch_job_data',key='job_search_filename')}}",
  data="{{task_instance.xcom_pull(task_ids='fetch_job_data',key='job_search_data')}}",
  replace=True,
  dag=dag,

)

fetch_data >> store_raw_to_s3
