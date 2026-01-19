import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta
    


from scripts.ingest_data import extract_data, transform_data, load_to_postgres 
from scripts.transform_to_s3 import upload_to_s3_as_parquet

def run_ingest_pipeline():
    raw = extract_data()
    clean = transform_data(raw)
    load_to_postgres(clean)

default_args = {
    'owner': 'bianca',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email':['biancadianazaharia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }
with DAG(
    dag_id='end_to_end_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

        # Task 1: python script 1 - cleaning and upload in postgres
        ingest_task = PythonOperator(
                task_id='ingest_and_basic_clean',
                python_callable=run_ingest_pipeline
        )


        # Task 2: python script 2 - Postgres -> S3 Bronze Layer
        upload_s3_task = PythonOperator(
                task_id='transform_parquet_and_upload',
                python_callable=upload_to_s3_as_parquet,
                op_kwargs={ 'bucket_name': 'etl-project3-data-warehouse-in-cloud'}
        )

        # Task 3
        bronze_layer = DatabricksRunNowOperator(
                task_id='databricks_bronze_processing',
                databricks_conn_id='databricks_default',
                job_id=89138455326258 
        )


        # Task 4: Databricks - Silver Layer processing
        silver_layer = DatabricksRunNowOperator(
                task_id='databricks_silver_processing',
                databricks_conn_id='databricks_default',
                job_id=341760643417237 
        )
        # Task 5: Databricks - Gold Layer processing
        gold_layer = DatabricksRunNowOperator(
                task_id='databricks_gold_modeling',
                databricks_conn_id='databricks_default',
                job_id=958544997832773
        )

        ingest_task >> upload_s3_task >> bronze_layer >> silver_layer >> gold_layer
