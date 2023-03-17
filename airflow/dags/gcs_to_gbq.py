from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
DIVVY_TRIPDATA_FOLDER = "divvy_tripdata"
URIS_FORMAT = f'gs://{BUCKET}/raw/{DIVVY_TRIPDATA_FOLDER}/{{}}/*.parquet'
TABLE_NAME = "raw_divvy_tripdata"
PARTITIONED_TABLE = TABLE_NAME.replace("raw_", "")
PARTITION_COL = "started_at"

create_table_query = f"""
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{PARTITIONED_TABLE}
PARTITION BY DATE({PARTITION_COL})
SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME};
"""

default_args = {
    'start_date': days_ago(1),
    'schedule_interval': None,
    'catchup': False,
}

with DAG('ingest_to_gbq_dag', default_args=default_args) as dag:
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

    create_partitioned_table_dag = BigQueryExecuteQueryOperator(
        task_id='create_table_task',
        sql=create_table_query,
        use_legacy_sql=False,
        location='us-east5',
        bigquery_conn_id='my_bigquery_connection',
    )

    chain(bigquery_external_table_task, create_partitioned_table_dag)