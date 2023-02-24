from helpers import convert_to_parquet, upload_to_gcs
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
import os


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_LOCAL_HOME_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

FILE_SUFFIX = {"CSV" : ".csv", "PARQUET" : ".parquet", "ZIP" : ".zip"}
URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com/"

EXECUTION_DATE = '{{ execution_date.strftime(\'%Y%m\') }}'
FILENAME = EXECUTION_DATE + '-divvy-tripdata' 
CSV_FILE = FILENAME + FILE_SUFFIX["CSV"]
PARQUET_FILE = FILENAME + FILE_SUFFIX["PARQUET"]
ZIP_FILE = FILENAME + FILE_SUFFIX["ZIP"]
URL_FORMATTED = URL_PREFIX + ZIP_FILE
DIVVY_TRIPDATA_GCS_PATH_TEMPLATE = f"raw/divvy_tripdata/{{{{ execution_date.strftime('%Y') }}}}/{PARQUET_FILE}"

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2020, 4, 30),
  'email': None,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'max_active_runs': 1,
  'catchup': True,
}

with DAG(
  dag_id="ingest_data_to_gcs_dag",
  schedule_interval='59 23 L * *', # Execute last day of the month at midnight
  default_args=default_args,
) as dag:
  
  download_dataset_task = BashOperator(
    task_id="download_dataset_task",
    bash_command=f"curl -sSL {URL_FORMATTED} > {AIRFLOW_LOCAL_HOME_PATH}/{ZIP_FILE}",
  )

  unzip_dataset_task = BashOperator(
    task_id="unzip_dataset_task",
    bash_command=f"unzip -o {AIRFLOW_LOCAL_HOME_PATH}/{ZIP_FILE} -d {AIRFLOW_LOCAL_HOME_PATH}",
  )

  format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet_task",
    python_callable=convert_to_parquet,
    op_kwargs={
      "src_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{CSV_FILE}",
      "dest_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{PARQUET_FILE}",
    },
   
  )

  upload_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
      "bucket" : BUCKET,
      "object_name" : DIVVY_TRIPDATA_GCS_PATH_TEMPLATE,
      "local_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{PARQUET_FILE}",
    },

  )

  remove_file_task = BashOperator(
    task_id="remove_file_task",
    bash_command=f"rm {AIRFLOW_LOCAL_HOME_PATH}/{FILENAME}*",
  )

  download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> remove_file_task
