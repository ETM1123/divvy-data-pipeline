# Explanation of dags 
 - Explanation of ingest_data_to_gcs dag:
    - Import all of the required libraries
      ```python
      from helpers import convert_to_parquet, upload_to_gcs
      from airflow.operators.python import PythonOperator
      from airflow.operators.bash import BashOperator
      from airflow import DAG
      from datetime import datetime
      import os
      ```
    - Get variables from env
      ```python
      PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
      BUCKET = os.environ.get("GCP_GCS_BUCKET")
      AIRFLOW_LOCAL_HOME_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
      ```
    - Declare Global variables that is going to be used throughout the program
      ```python
      FILE_SUFFIX = {"CSV" : ".csv", "PARQUET" : ".parquet", "ZIP" : ".zip"}
      URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com/"

      EXECUTION_DATE = '{{ execution_date.strftime(\'%Y%m\') }}'
      FILENAME = EXECUTION_DATE + '-divvy-tripdata' 
      CSV_FILE = FILENAME + FILE_SUFFIX["CSV"]
      PARQUET_FILE = FILENAME + FILE_SUFFIX["PARQUET"]
      ZIP_FILE = FILENAME + FILE_SUFFIX["ZIP"]
      URL_FORMATTED = URL_PREFIX + ZIP_FILE
      DIVVY_TRIPDATA_GCS_PATH_TEMPLATE = f"raw/divvy_tripdata/{{{{ execution_date.strftime('%Y') }}}}/{PARQUET_FILE}"
      ```
        -  Note: the variables above are used for formatting filenames, paths, and urls. 
          - In our case, the biketrip data is stored in zipfile in divvy website organized by year and month. The execution data allows to get the correct file.
        - Durning execution, airflow can interpret jinja code, which allows us to get the execution date
    - Define default dags args
      ```python
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
      ```
        - In our case the two most important arguments are:
          - start_date since there are no data available in the webpage prior to April of 20202
          - catchup: This backfills all of the missed executions. For instance, if this was false we won't be able to get all the data since the start_date is in the past. 
  - Define dag with default variables:
    ```python
    with DAG(
      dag_id="ingest_data_to_gcs_dag",
      schedule_interval='59 23 L * *', 
      default_args=default_args,
    ) as dag:
    ```
      - The schedule_interval argument is crucial for our program. In our case we want trigger our scheduler at the end of every month. We can accomplish this task using a cron expression. Specifically, the cron expression'59 23 L * *' will trigger a task at 11:59 PM on the last day of every month, regardless of the day of the week. 
  - Add first task 
    ```python
    download_dataset_task = BashOperator(
      task_id="download_dataset_task",
      bash_command=f"curl -sSL {URL_FORMATTED} > {AIRFLOW_LOCAL_HOME_PATH}/{ZIP_FILE}",
    )
    ```
      - Note: For any dag task you need a task_id. Since this task uses bash commands we need to use the BashOperator. 
  - Add second task 
    ```python
    unzip_dataset_task = BashOperator(
    task_id="unzip_dataset_task",
    bash_command=f"unzip -o {AIRFLOW_LOCAL_HOME_PATH}/{ZIP_FILE} -d {AIRFLOW_LOCAL_HOME_PATH}",
    )
    ```
      - Note: In this task we use the unzip bash command. The unzip bash command is not a default command for all os. Therefore, installing unzip command with our Dockerfile allows us to use the unzip command in this dag. In other words, ff we didn't have unzip command, then this task will fail.
  - Add third task 
    ```python
    format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet_task",
    python_callable=convert_to_parquet,
    op_kwargs={
      "src_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{CSV_FILE}",
      "dest_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{PARQUET_FILE}",
      },
    )
    ```
      - Note: 
        - Now we are using the PythonOperator since we are passing our custom convert_to_parquet function which can be found in the helpers module. We can pass the function arguments using the op_kwargs argument. 
        - Convert_to_parquet is a simple function that takes a csv file and converts it to a parquet file using pandas module. 
        - Parquet is a columnar storage file format designed for efficient and scalable processing of large data sets. It is often used to store data in cloud services. 
  - Add fourth task 
    ```python
    upload_to_gcs_task = PythonOperator(
      task_id="upload_to_gcs_task",
      python_callable=upload_to_gcs,
      op_kwargs={
        "bucket" : BUCKET,
        "object_name" : DIVVY_TRIPDATA_GCS_PATH_TEMPLATE,
        "local_file" : f"{AIRFLOW_LOCAL_HOME_PATH}/{PARQUET_FILE}",
      },
    )
    ```
      -   Note:
          - This task takes the converted parquet file and uploads it to our GCS. We added additional file storage formatting with the `DIVVY_TRIPDATA_GCS_PATH_TEMPLATE` which stores each file in the following file format: raw/divvy_tripdata/<data_year>/<filename.parquet> in our GCS.

  - Add fifth task 
    ```python
    remove_file_task = BashOperator(
      task_id="remove_file_task",
      bash_command=f"rm {AIRFLOW_LOCAL_HOME_PATH}/{FILENAME}*",
    )
    ```
      - Note:
          - This is not a necessarily task, we just added to clean up the files in our image container. 
  - Add dependencies i.e execution order
    ```python
    download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> remove_file_task
    ``` 
    - Note:
      - This is very important. This dictates the execution order. 


     - Explanation of gcs_to_gbq dag
      - Import necessarily packages but to execute 
          ```python
          from datetime import datetime
          from airflow import DAG
          from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
          from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
          from airflow.models.baseoperator import chain
          from airflow.utils.dates import days_ago
          import os
          ```
      - Declare global variables
          ```python
          PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
          BUCKET = os.environ.get("GCP_GCS_BUCKET")
          BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
          DIVVY_TRIPDATA_FOLDER = "divvy_tripdata"
          URIS_FORMAT = f'gs://{BUCKET}/raw/{DIVVY_TRIPDATA_FOLDER}/{{}}/*.parquet'
          TABLE_NAME = "raw_divvy_tripdata"
          PARTITIONED_TABLE = TABLE_NAME.replace("raw_", "")
          PARTITION_COL = "started_at"
          ```
      - Declare query
          ```python
          create_table_query = f"""
          -- Creating a partition table
          CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{PARTITIONED_TABLE}
          PARTITION BY DATE({PARTITION_COL})
          SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME};
          """
          ```
      - Default variables 
          ```python
          default_args = {
          'start_date': days_ago(1),
          'schedule_interval': None,
          'catchup': False,
          }
          ```
      - Define dag
          ```python
          with DAG('ingest_to_gbq_dag', default_args=default_args) as dag:
          ```
      - Add task 1:
          ```python
          bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TABLE_NAME,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        URIS_FORMAT.format(year) for year in range(2020, 2023)
                    ],
                },
            },
          )
          ```
      - Add task 2
          ```python
          create_partitioned_table_dag = BigQueryExecuteQueryOperator(
          task_id='create_table_task',
          sql=create_table_query,
          use_legacy_sql=False,
          location='us-east5',
          bigquery_conn_id='my_bigquery_connection',
          )
          ```
          - Note:
            - This task depends on the bigquery connection, if the connection is not set up correctly, this task wil fail
            - Partitioned tables in BigQuery provide an efficient and cost-effective way to manage and query large datasets.
      - Add dependencies 
          ```python
          chain(bigquery_external_table_task, create_partitioned_table_dag)
          ```