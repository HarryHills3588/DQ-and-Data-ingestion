import sys
sys.path.insert(0, "/Users/harryhillsdownley/Desktop/CWRU/CSDS 397/DQ and Data ingestion")

from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from src.airflow.ingestion.ingestion import ingest_employee_data

with DAG(
    "employee_etl_pipeline",
    default_args={
        "owner": "harry_hillsdownley",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "max_active_tasks": 1,
        "email_on_failure": True, 
        "email_on_retry": True,
        "email": ['hsh47@case.edu']
    },
    start_date=datetime(2026, 3, 3),
    schedule="@daily",
    catchup=False,
    description="ETL pipeline: ingest, clean/normalize, then run transformation models",
    tags = ['etl','ingestion']
) as dag:

    ingestion = PythonOperator(
        task_id="ingest_employee_data",
        python_callable=ingest_employee_data
    )
    
    cln_anlytcs = BashOperator(
        task_id="cleaning_and_analytics",
        bash_command='cd "/Users/harryhillsdownley/Desktop/CWRU/CSDS 397/DQ and Data ingestion/src/airflow/CSDS397" && dbt run',
    )
    
    

    # ingestion >> # task2 runs after task
    
    ingestion >> cln_anlytcs
    
