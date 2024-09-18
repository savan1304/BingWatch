from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import boto3
import sys
import os
from src.trailer_extract import process_csv
from src.data_cleaning import clean
from src.snowflake_upload import sf_upload


api_key = os.getenv("TMDB_API_KEY")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_trailer_task():
    process_csv('/opt/airflow/src/dataset.csv','/opt/airflow/src/final_dataset.csv')

def data_preprocessing_task():
    clean('/opt/airflow/src/final_dataset.csv','/opt/airflow/src/final_movies.csv','/opt/airflow/src/final_tvshows.csv')

def upload_to_snowflake_task():
    sf_upload('/opt/airflow/src/final_movies.csv','/opt/airflow/src/final_tvshows.csv')


with DAG(
    'data_extraction',
    default_args=default_args,
    description='A consolidated dag for ETL',
    schedule=None,
    start_date=datetime(2024, 4, 21),
    catchup=False,
) as dag:
    
    extract_data_task= PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_trailer_task,
    )

    clean_data_task= PythonOperator(
        task_id='clean_data_task',
        python_callable=data_preprocessing_task,
    )

    upload_snowflake_task= PythonOperator(
        task_id='upload_to_snowflake_task',
        python_callable=upload_to_snowflake_task,
    )
    

    
    extract_data_task >> clean_data_task >> upload_snowflake_task

